Esse repositório demonstra como criar um dashboard usando Streamlit na linguagem de programação Python.
A importância do dashboard se demonstra na maior clareza que os dados ganham para usuários comuns, como a população no geral, como também para usuários que trabalham diretamente com esses dados e com essa maior clareza, podem entender o que está acontecendo e tomar melhores decisões sobre os mesmos.

No contexto desse projeto, PyPAH, estamos usando dados da saúde, vindos do DATASUS, banco de dados do SUS(Sistema Único de Saúde) do Governo do Brasil. Mais precisamente, estamos demonstrando uma forma de lidar com os dados vindo do Sistema de Internação Ambulatorial(SIA), a Produção Ambulatorial(PA) com Python, fazendo todo o processo de Extração do FTP do DATASUS, Transformação dos dados com limpeza e filtros, por fim a Carga do resultado obtido em um banco de dados que servirá o Streamlit, biblioteca do Python que gera uma aplicação web de dashboard.

Requisitos para utilização correta desse projeto:

- Ter instalado o Python.
- Ter instalado o Docker.
- Seu Sistema Operacional deve ser Linux ou caso seja Windows, tenha o WSL instalado.

O Python deve ser a versão 3.11
Recomendo que o Docker e WSL estejam na mesma versão que usei, sendo:
- Docker 29.2.1
- WSL 2.6.1.0	

WSL(Windows SubSystem for Linux) servirá para executar um sistema Linux nativo no Windows, que é necessário para utilização do Docker e da biblioteca PySUS.
Docker será utilizado para a criação do ambiente virtual que instalará as dependências na sua máquina.


Recomendo também que tenha instalado uma IDE, como o Visual Studio Code para executar os códigos, mas caso queira, também pode executar apenas via terminal.


## Organização das Pastas do Projeto

```text
PyPAH
│
├── dados_sia/
│   ├── dados_dbc/
│   ├── Bronze/
│   ├── Silver/
│   ├── Gold/
│   └── rótulos/
│
├── db/
│   └── db.duckdb
│
├── Docker/
│   ├── Dockerfile.dev
│   ├── Dockerfile.user
│   └── entrypoint_user.sh
│
├── ETL/
│   ├── fun_sia.py
│   ├── gold.py
│
├── requirements/
│   ├── requirements_dev.txt
│   └── requirements_user.txt
│
├── Streamlit/
│   └── dash_PyPAH.py
│
├── .dockerignore
├── docker-compose.yml
└── .gitignore
```text


A pasta dados_sia é onde ficam guardados os dados que serão utilizados dentro do projeto
Ela contém 5 pastas internas:
dados_dbc sendo onde serão guardados os dados brutos ainda compactados em .dbc baixados do FTP do DataSUS.
Bronze onde ficam os dados descompactados já convertidos em Parquet, cada pasta dentro dele será referente a um mês.
Silver onde ficam os dados após filtragem, tratamento e adição de colunas.
Gold onde ficam os dados finais depois de agregados em tabelas menores prontos para uso no Streamlit.	
rótulos onde ficam as tabelas dimensão (como se fosse um dicionário) para rotular colunas nos filtros do dashboard

A pasta db é onde fica guardado um arquivo db.duckdb que é um banco de dados otimizado para o dashboard, dentro dele ficam as tabelas Gold

A pasta Docker contém os arquivos responsáveis pela containerização do Docker
Temos o Dockerfile.dev que criará um container com as dependências integrais para todo o projeto.
O Dockerfile.user, também cria um container, mas apenas com as dependências para abrir o dashboard, sendo esse container menor e mais rápido de ser instalado.
O entrypoint_user.sh é executado automaticamente quando é criado o container de user, ele abrirá automaticamente o dashboard na porta designada no arquivo, mas antes ele faz um check se localmente já possui o arquivo db.duckdb, que é o banco de dados que serve o streamlit como disse anteriormente, caso não haja, ele baixa automaticamente o release com 3 anos que fiz no GitHub.

A pasta ETL, como o nome sugere, é onde ficam os arquivos que fazem a ETL dos dados
No fun_sia.py estão as funções e as chamadas dela para criar os bancos Bronze e Silver e baixar os rótulos.
Já em gold.py é onde o banco gold é criado no db.duckdb.

A pasta requirements tem dois arquivos .txt que designam as dependências que devem ser instaladas e as versões adequadas para cada container.

A pasta Streamlit tem dentro dela o arquivo que executa o app, a partir da execução do dash_PyPAH.py podemos visualizar o resultado de todo o processo de ETL e acessar os dados e utilizar os filtros para melhorar o entendimento dos mesmos.

.dockerignore sendo um arquivo que fará o Docker ignorar pastas e arquivos designados dentro do mesmo
.gitignore o mesmo, mas para o GitHub

docker-compose.yml é onde está sendo configurado a criação dos containers

A princípio, as pastas dados_sia/ e db/ não estão no GitHub, mas na execução do fluxo eles serão criados, no fun_sia, dados_sia será criado automaticamente e em gold.py, db/ será criado.



comandos para executar o projeto:

clonar o projeto do GitHub localmente:
```text
git clone https://github.com/repositorio-paineis-publicos/PyPAH
```text

abra a aplicação do Docker Desktop na sua máquina

no terminal do VS Code e no próprio terminal, abra a pasta do projeto
pode usar: 

```text
cd caminho_do_projeto #(substitua pelo caminho real)
```text

caso esteja em um SO Windows
escreva:
```text
wsl #ativa o wsl no projeto
```text
e então no terminal execute o comando:

```text
docker compose up --build -d pypah-dev #(ou pypah-user, caso queira acessar só o dash)
```text
nesse momento, o seu container começará a ser criado, a instalação de dependências, ao final terá uma mensagem de confirmação da criação do container

após isso, clique no atalho 'Ctrl + Shift + P' para abrir a barra do VS
escreva 'Dev Containers: Attach to Running Container', clique
aparecerá o nome do container, clique nele
abrirá uma nova aba do VS agora com o ambiente virtual pronto para execução

para executar o dash, execute o comando:

```text
streamlit run Streamlit/dash_PyPAH.py
```text

automaticamente será aberto no seu navegador o app
caso não abra automaticamente, escreva no navegador:
localhost:numero_da_porta

fique atento ao número das portas configuradas quando acessar o Streamlit, pois o container de user e dev tem portas mapeadas diferentes
quando é o do user, a porta é 8501 e de dev 8502, caso queira alterar, mude no arquivo docker-compose.yml

