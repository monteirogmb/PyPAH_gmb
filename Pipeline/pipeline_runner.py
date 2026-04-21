"""
pipeline_runner.py
------------------
Orquestrador do pipeline incremental do PyPAH.

Fluxo de execução:
  1. Conecta ao R2 e lista quais partições (ano/mes) já existem no bucket.
  2. Calcula quais meses novos estão disponíveis no FTP do DATASUS.
  3. Para cada mês novo: baixa .dbc → converte → trata → faz upload da partição gold no R2.
  4. Atualiza também as tabelas dimensão (rótulos) se necessário.

Variáveis de ambiente obrigatórias (definidas no .env ou no painel do Render):
  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET

Uso:
  # Rodar normalmente (só meses novos):
  python -m Pipeline.pipeline_runner

  # Forçar reprocessamento de um range histórico específico:
  python -m Pipeline.pipeline_runner --ano-inicio 2022 --mes-inicio 1 --ano-fim 2024 --mes-fim 12
"""

import os
import shutil
import argparse
import logging
from datetime import date
from pathlib import Path
from dateutil.relativedelta import relativedelta

import boto3
from botocore.config import Config
from dotenv import load_dotenv

from Pipeline.fun_sia import (
    baixar_dbc,
    conv_dbc_para_pqt,
    tratar_dados_sia,
    download_proc_label,
    estab_ce_label,
    col_interesse,
)
from Pipeline.gold import processar_gold_particionado

# ─────────────────────────────────────────────
# Configuração de logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────
load_dotenv()

GRUPO   = "PA"
ESTADO  = "CE"

# Pastas temporárias (dentro do container — apagadas após cada mês processado)
BASE_TMP    = Path("/tmp/pypah")
PASTA_DBC   = BASE_TMP / "dbc"
PASTA_BRONZE = BASE_TMP / "bronze"
PASTA_SILVER = BASE_TMP / "silver"
PASTA_ROTULOS = BASE_TMP / "rotulos"

# Prefixo no R2 onde ficam as partições gold e as dimensões
R2_GOLD_PREFIX = "gold"
R2_DIMS_PREFIX = "dims"

# O DATASUS normalmente disponibiliza os dados com ~2 meses de atraso.
# Este valor determina quantos meses atrás o pipeline considera como "disponível".
MESES_ATRASO_DATASUS = 2


# ─────────────────────────────────────────────
# Helpers do R2 (S3-compatible via boto3)
# ─────────────────────────────────────────────

def _s3_client():
    """Cria e retorna um cliente boto3 configurado para o Cloudflare R2."""
    endpoint = os.environ["R2_ENDPOINT"]
    # O endpoint do R2 pode vir sem o schema — garantimos que tem https://
    if not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def listar_particoes_existentes(s3, bucket: str) -> set[tuple[int, int]]:
    """
    Lista as partições gold que já existem no R2.

    Espera a estrutura:
        gold/ano=YYYY/mes=MM/dados.parquet

    Retorna um set de tuplas (ano, mes) que já foram processadas.
    """
    existentes = set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=f"{R2_GOLD_PREFIX}/ano="):
        for obj in page.get("Contents", []):
            # Exemplo de chave: gold/ano=2024/mes=03/dados.parquet
            partes = obj["Key"].split("/")
            try:
                ano = int(partes[1].replace("ano=", ""))
                mes = int(partes[2].replace("mes=", ""))
                existentes.add((ano, mes))
            except (IndexError, ValueError):
                continue

    log.info(f"Partições já existentes no R2: {len(existentes)}")
    return existentes


def calcular_meses_disponiveis(ano_inicio: int, mes_inicio: int) -> list[tuple[int, int]]:
    """
    Retorna a lista de (ano, mes) desde (ano_inicio, mes_inicio) até
    o último mês que o DATASUS já deveria ter disponível (hoje - MESES_ATRASO_DATASUS).
    """
    hoje = date.today()
    limite = hoje - relativedelta(months=MESES_ATRASO_DATASUS)
    limite_tuple = (limite.year, limite.month)

    cursor = date(ano_inicio, mes_inicio, 1)
    meses = []

    while (cursor.year, cursor.month) <= limite_tuple:
        meses.append((cursor.year, cursor.month))
        cursor += relativedelta(months=1)

    return meses


def fazer_upload_particao(s3, bucket: str, arquivo_local: Path, ano: int, mes: int):
    """Faz upload do parquet silver processado como uma partição gold no R2."""
    chave = f"{R2_GOLD_PREFIX}/ano={ano}/mes={mes:02d}/dados.parquet"
    log.info(f"Fazendo upload → s3://{bucket}/{chave}")
    s3.upload_file(str(arquivo_local), bucket, chave)
    log.info("Upload concluído.")


def fazer_upload_dim(s3, bucket: str, arquivo_local: Path, nome_arquivo: str):
    """Faz upload de uma tabela dimensão para o prefixo dims/ no R2."""
    chave = f"{R2_DIMS_PREFIX}/{nome_arquivo}"
    log.info(f"Fazendo upload da dimensão → s3://{bucket}/{chave}")
    s3.upload_file(str(arquivo_local), bucket, chave)


# ─────────────────────────────────────────────
# Pipeline por mês
# ─────────────────────────────────────────────

def processar_mes(s3, bucket: str, ano: int, mes: int):
    """
    Executa o pipeline completo para um único mês:
      bronze → silver → gold particionado no R2

    Cria pastas temporárias, processa, faz upload e limpa tudo ao final.
    """
    log.info(f"{'='*50}")
    log.info(f"Processando {ano}/{mes:02d}...")
    log.info(f"{'='*50}")

    # Pastas temporárias isoladas por mês (evita conflito entre runs)
    pasta_dbc_mes    = PASTA_DBC    / f"{ano}{mes:02d}"
    pasta_bronze_mes = PASTA_BRONZE / f"{ano}{mes:02d}"
    pasta_silver_mes = PASTA_SILVER / f"{ano}{mes:02d}"

    for p in [pasta_dbc_mes, pasta_bronze_mes, pasta_silver_mes]:
        p.mkdir(parents=True, exist_ok=True)

    arquivo_silver = pasta_silver_mes / "silver.parquet"

    try:
        # 1. Download do .dbc do FTP do DATASUS
        log.info("Etapa 1/3 — Download do FTP DATASUS...")
        baixar_dbc(
            grupo=GRUPO,
            estado=ESTADO,
            anos=[ano],
            meses=[mes],
            destino=pasta_dbc_mes,
        )

        # Verifica se o arquivo foi realmente baixado (mês pode não existir no FTP ainda)
        arquivos_baixados = list(pasta_dbc_mes.glob("*.dbc"))
        if not arquivos_baixados:
            log.warning(f"Nenhum .dbc encontrado para {ano}/{mes:02d}. Pulando.")
            return False

        # 2. Conversão .dbc → .parquet (camada bronze)
        log.info("Etapa 2/3 — Conversão DBC → Parquet (bronze)...")
        conv_dbc_para_pqt(
            pasta_origem=str(pasta_dbc_mes),
            pasta_destino=str(pasta_bronze_mes),
        )

        # 3. Tratamento e geração do silver (filtros, colunas derivadas, agregação)
        log.info("Etapa 3/3 — Tratamento e geração do Silver...")
        tratar_dados_sia(
            pasta=str(pasta_bronze_mes),
            colunas=col_interesse,
            arquivo_saida=str(arquivo_silver),
            verbose=True,
        )

        if not arquivo_silver.exists():
            log.error(f"Arquivo silver não foi gerado para {ano}/{mes:02d}.")
            return False

        # 4. Upload da partição gold para o R2
        fazer_upload_particao(s3, bucket, arquivo_silver, ano, mes)
        log.info(f"Mês {ano}/{mes:02d} processado com sucesso.")
        return True

    except Exception as e:
        log.error(f"Erro ao processar {ano}/{mes:02d}: {e}", exc_info=True)
        return False

    finally:
        # Limpa arquivos temporários do mês para não acumular disco
        for pasta in [pasta_dbc_mes, pasta_bronze_mes, pasta_silver_mes]:
            if pasta.exists():
                shutil.rmtree(pasta)
                log.debug(f"Pasta temporária removida: {pasta}")


# ─────────────────────────────────────────────
# Atualização das tabelas dimensão
# ─────────────────────────────────────────────

def atualizar_dimensoes(s3, bucket: str):
    """
    Baixa e faz upload das tabelas dimensão (estabelecimentos e procedimentos).
    Roda sempre que o pipeline principal é executado, pois os rótulos podem mudar.
    """
    log.info("Atualizando tabelas dimensão...")
    PASTA_ROTULOS.mkdir(parents=True, exist_ok=True)

    try:
        path_estab = estab_ce_label(destino=PASTA_ROTULOS)
        fazer_upload_dim(s3, bucket, Path(path_estab), "dim_estabelecimento_ce.parquet")
    except Exception as e:
        log.error(f"Erro ao atualizar dim_estabelecimento: {e}", exc_info=True)

    try:
        path_proc = download_proc_label(destino=PASTA_ROTULOS)
        fazer_upload_dim(s3, bucket, Path(path_proc), "dim_procedimento.parquet")
    except Exception as e:
        log.error(f"Erro ao atualizar dim_procedimento: {e}", exc_info=True)

    # Limpa pasta de rótulos após upload
    if PASTA_ROTULOS.exists():
        shutil.rmtree(PASTA_ROTULOS)


# ─────────────────────────────────────────────
# Ponto de entrada
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Pipeline incremental PyPAH")
    parser.add_argument("--ano-inicio",  type=int, default=None, help="Ano de início da carga histórica (ex: 2018)")
    parser.add_argument("--mes-inicio",  type=int, default=None, help="Mês de início da carga histórica (ex: 1)")
    parser.add_argument("--ano-fim",     type=int, default=None, help="Ano de fim (ex: 2024). Padrão: hoje - 2 meses")
    parser.add_argument("--mes-fim",     type=int, default=None, help="Mês de fim (ex: 12). Padrão: hoje - 2 meses")
    parser.add_argument("--skip-dims",   action="store_true",    help="Pula a atualização das tabelas dimensão")
    args = parser.parse_args()

    bucket = os.environ["R2_BUCKET"]
    s3     = _s3_client()

    # ── Determinar quais meses processar ──────────────────────────────────────

    particoes_existentes = listar_particoes_existentes(s3, bucket)

    if args.ano_inicio and args.mes_inicio:
        # Modo carga histórica: processa o range completo informado via argumento
        ano_inicio = args.ano_inicio
        mes_inicio = args.mes_inicio
        log.info(f"Modo carga histórica: a partir de {ano_inicio}/{mes_inicio:02d}")
    else:
        # Modo incremental padrão: começa a partir do mínimo histórico configurado
        # Se já há partições no R2, começa do mês seguinte à última partição existente
        if particoes_existentes:
            ultimo_ano, ultimo_mes = max(particoes_existentes)
            proximo = date(ultimo_ano, ultimo_mes, 1) + relativedelta(months=1)
            ano_inicio, mes_inicio = proximo.year, proximo.month
            log.info(f"Modo incremental: continuando a partir de {ano_inicio}/{mes_inicio:02d}")
        else:
            # Primeira execução sem nenhuma partição: usa Jan/2018 como ponto de partida padrão
            ano_inicio, mes_inicio = 2018, 1
            log.info("Nenhuma partição existente. Iniciando carga histórica desde 2018/01.")

    todos_os_meses = calcular_meses_disponiveis(ano_inicio, mes_inicio)

    # Filtra somente os meses que ainda não existem no R2
    meses_pendentes = [(a, m) for a, m in todos_os_meses if (a, m) not in particoes_existentes]

    # Se foi especificado um fim, filtra também pelo limite superior
    if args.ano_fim and args.mes_fim:
        limite = (args.ano_fim, args.mes_fim)
        meses_pendentes = [(a, m) for a, m in meses_pendentes if (a, m) <= limite]

    if not meses_pendentes:
        log.info("Nenhum mês novo para processar. Pipeline encerrado.")
        return

    log.info(f"Meses a processar: {len(meses_pendentes)}")
    for a, m in meses_pendentes:
        log.info(f"  → {a}/{m:02d}")

    # ── Processar cada mês ───────────────────────────────────────────────────
    sucessos  = 0
    falhas    = 0

    for ano, mes in meses_pendentes:
        ok = processar_mes(s3, bucket, ano, mes)
        if ok:
            sucessos += 1
        else:
            falhas += 1

    log.info(f"Pipeline concluído. Sucessos: {sucessos} | Falhas: {falhas}")

    # ── Atualizar dimensões ──────────────────────────────────────────────────
    if not args.skip_dims:
        atualizar_dimensoes(s3, bucket)

    log.info("Tudo pronto! Os dados no R2 estão atualizados.")


if __name__ == "__main__":
    main()
