"""
gold.py
-------
Funções de geração da camada Gold.

Na nova arquitetura, a camada Gold é composta por partições mensais no R2:
    gold/ano=YYYY/mes=MM/dados.parquet

Esta função é chamada internamente pelo pipeline_runner, que já cuida do upload.
Aqui ficam funções auxiliares de agregação/transformação para a camada Gold,
caso seja necessário no futuro criar tabelas mais agregadas a partir das partições.

A função principal usada pelo pipeline_runner é `processar_gold_particionado`,
que recebe o arquivo silver de um mês e aplica a mesma lógica de agregação
que o gold.py original fazia via DuckDB — mas gerando um parquet local
pronto para upload.
"""

import duckdb
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def processar_gold_particionado(
    arquivo_silver: str | Path,
    arquivo_saida: str | Path,
) -> Path:
    """
    Aplica a agregação Gold em cima de um arquivo silver de um único mês
    e salva o resultado como parquet local.

    A lógica de agregação é idêntica à tabela `gold_fact_qtd_val_TT` original:
    agrupa por unidade, ano, mês, data, município e procedimento,
    somando valores e quantidades produzidas/aprovadas.

    Parâmetros
    ----------
    arquivo_silver : caminho para o parquet silver do mês
    arquivo_saida  : caminho onde o parquet gold será salvo

    Retorna o Path do arquivo de saída.
    """
    arquivo_silver = str(arquivo_silver)
    arquivo_saida  = str(arquivo_saida)

    con = duckdb.connect()

    log.info(f"Gerando Gold a partir de: {arquivo_silver}")

    con.execute(f"""
        COPY (
            SELECT
                PA_CODUNI,
                Ano,
                Mes,
                data_ref,
                PA_MUNPCN,
                PA_PROC_ID,
                SUM(CAST(PA_VALPRO AS DOUBLE))  AS PA_VALPRO,
                SUM(CAST(PA_VALAPR AS DOUBLE))  AS PA_VALAPR,
                SUM(CAST(PA_QTDPRO AS BIGINT))  AS PA_QTDPRO,
                SUM(CAST(PA_QTDAPR AS BIGINT))  AS PA_QTDAPR
            FROM read_parquet('{arquivo_silver}')
            GROUP BY
                PA_CODUNI,
                Ano,
                Mes,
                data_ref,
                PA_MUNPCN,
                PA_PROC_ID
        ) TO '{arquivo_saida}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)

    con.close()

    log.info(f"Gold gerado em: {arquivo_saida}")
    return Path(arquivo_saida)


def consolidar_gold_local(
    pasta_particoes: str | Path,
    arquivo_saida: str | Path,
) -> Path:
    """
    [Utilitário opcional]

    Consolida todas as partições Gold locais (de uma carga histórica)
    em um único arquivo parquet, usando DuckDB.

    Útil para gerar um snapshot completo sem precisar acessar o R2.

    Parâmetros
    ----------
    pasta_particoes : pasta com subpastas ano=YYYY/mes=MM/dados.parquet
    arquivo_saida   : caminho do parquet consolidado final
    """
    pasta_particoes = str(pasta_particoes)
    arquivo_saida   = str(arquivo_saida)

    con = duckdb.connect()

    log.info(f"Consolidando partições de: {pasta_particoes}")

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{pasta_particoes}/**/*.parquet', hive_partitioning=true)
            ORDER BY data_ref
        ) TO '{arquivo_saida}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)

    con.close()

    log.info(f"Consolidação concluída: {arquivo_saida}")
    return Path(arquivo_saida)