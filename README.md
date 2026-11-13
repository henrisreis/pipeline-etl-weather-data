# Pipeline ETL: Dados Meteorológicos de Barreiras-BA | Docker, Airflow, Postgres e Python

Pipeline ETL de condições meteorológicas para Barreiras, Bahia. O projeto consulta a API OpenWeatherMap, normaliza a resposta e grava os dados em uma tabela PostgreSQL. O fluxo pode ser executado manualmente ou orquestrado pelo Apache Airflow.

## Conteúdo

- [Arquitetura e fluxo](#arquitetura-e-fluxo)
- [Tecnologias e pré-requisitos](#tecnologias-e-pré-requisitos)
- [Configuração](#configuração)
- [Execução local](#execução-local)
- [Execução com Airflow](#execução-com-airflow)
- [Dados produzidos](#dados-produzidos)
- [Operação e limitações conhecidas](#operação-e-limitações-conhecidas)
- [Estrutura do projeto](#estrutura-do-projeto)

## Arquitetura e fluxo

A arquitetura do projeto é composta por cinco componentes bem definidos:

- Fonte: API OpenWeatherMap
- Processamento: Python
- Orquestração: Airflow
- Infra: Docker
- Banco de dados: PostgreSQL

```text
┌──────────────────────────────┐
│ Fonte: API OpenWeatherMap    │
│ Consulta de clima em tempo   │
│ real para Barreiras, BA      │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Processamento: Python        │
│ - extração                   │
│ - transformação              │
│ - carga                      │
│ - persistência local em JSON │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Orquestração: Airflow        │
│ DAG weather_pipeline         │
│ Agenda e executa o pipeline  │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Infra: Docker                │
│ Containers do Airflow, Redis │
│ e serviços de suporte        │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Banco de dados: PostgreSQL   │
│ tabela: barreiras_weather    │
└──────────────────────────────┘
```

O pipeline coleta o clima atual nas coordenadas `-12.1482, -44.9925`, usa unidades métricas e solicita descrições em português (`pt-BR`).

## Tecnologias e pré-requisitos

- Python `>=3.10` (a versão selecionada no arquivo `.python-version` é `3.14`).
- [uv](https://docs.astral.sh/uv/) para sincronizar as dependências Python.
- Uma chave de API do [OpenWeatherMap](https://openweathermap.org/api).
- PostgreSQL acessível em `host.docker.internal:5432`, conforme configurado em `src/pipeline_weather/load_data.py`.
- Para executar o orquestrador: Docker e Docker Compose.

O PostgreSQL definido no `docker-compose.yaml` é usado pelo Airflow para seus metadados. Ele **não** é o banco de destino do pipeline: a carga usa o host `host.docker.internal` e o banco especificado em `DATABASE`. Em Linux, esse nome pode não estar disponível por padrão; configure a resolução de host/rede do Docker ou ajuste o endpoint no código para o seu ambiente.

## Configuração

Crie `config/.env` (não versionado) com as credenciais da aplicação:

```dotenv
API_KEY=sua_chave_openweathermap
USER=usuario_postgres
PASSWORD=senha_postgres
DATABASE=nome_do_banco
```

O usuário do banco precisa ter permissões para criar/inserir dados na tabela e consultar seus registros. O SQLAlchemy conecta à porta `5432`; o host é atualmente fixo como `host.docker.internal`.

Para executar com Docker Compose, crie também um `.env` na raiz do projeto definindo o identificador do seu usuário (`AIRFLOW_UID`):

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Ele é um passo padrão da configuração oficial do Apache Airflow via Docker Compose, usado para evitar problemas de permissão ao criar e ler arquivos (como logs e DAGs) compartilhados entre os containers e a sua máquina. As credenciais padrão locais do Airflow são `airflow`/`airflow`; altere-as no `.env` da raiz (`_AIRFLOW_WWW_USER_USERNAME` e `_AIRFLOW_WWW_USER_PASSWORD`) antes de iniciar o ambiente.

## Execução local

Na raiz do repositório, instale/sincronize as dependências e execute o script:

```bash
uv sync
uv run main.py
```

O script executa extração, transformação e carga em sequência. A saída bruta é gravada em `data/weather_data.json`, e o DataFrame transformado é enviado para `barreiras_weather`.

## Execução com Airflow

O Compose sobe o Airflow com CeleryExecutor, Redis e um PostgreSQL para metadados. Para iniciar:

```bash
docker compose up -d
```

Para verificar se todos os serviços estão sendo executados, seu status no terminal deve ser `healthy` ou `running`.

A interface fica disponível em <http://localhost:8080>. Entre com as credenciais locais configuradas no `.env` da raiz caso tenham sido configuradas (caso contrário, utilize as credenciais padrão locais do Airflow `airflow`/`airflow`).

O DAG `weather_pipeline` está definido em `dags/weather_dag.py`. Ele está programado para executar no início de cada hora (`0 */1 * * *`), não faz catchup e tenta novamente até duas vezes, com cinco minutos de intervalo. DAGs são criados pausados por padrão nesta configuração; habilite `weather_pipeline` na interface para iniciar os agendamentos ou dispare uma execução manual.

O DAG compartilha `data/weather_data.json` e `data/temp_data.parquet` por meio do volume `data/`. O arquivo Parquet é uma etapa intermediária entre as tarefas de transformação e carga.

Para parar os serviços:

```bash
docker compose down
```

Para parar e remover volumes (dados serão apagados):

```bash
docker compose down -v
```

Remover dados do Airflow e manter o PostgreSQL:

```bash
docker compose down
rm -rf logs/*
```

## Dados produzidos

A transformação normaliza as informações meteorológicas em colunas tabulares, incluindo cidade e país, coordenadas, temperatura, sensação térmica, umidade, pressão, visibilidade, vento, nuvens e horário de nascer/pôr do sol. As colunas de horários Unix (`datetime`, `sunrise` e `sunset`) são convertidas para timestamps no fuso `America/Sao_Paulo`.

A carga usa `pandas.DataFrame.to_sql` com `if_exists="append"` e sem índice. Se a tabela ainda não existir, o pandas/SQLAlchemy tentará criá-la; se já existir, os novos registros serão acrescentados. Não há chave de deduplicação nem operação de upsert.

## Operação e limitações conhecidas

- Cada extração bem-sucedida sobrescreve `data/weather_data.json`; o arquivo representa a resposta mais recente, não um histórico.
- Cada carga acrescenta registros à tabela. Execuções repetidas podem inserir observações duplicadas.
- Se a API falhar, a extração retorna uma lista vazia. A transformação subsequente pode então encontrar um arquivo de uma execução anterior (ou falhar se não houver arquivo), portanto verifique os logs e a atualidade dos dados após uma falha de extração.
- A execução local captura exceções, registra o erro e imprime o traceback, mas não propaga a exceção ao processo chamador. Confirme os logs e o resultado no banco; não use somente o código de saída do processo como confirmação de sucesso.
- O Compose fornecido é destinado a desenvolvimento local. As credenciais padrão do Airflow e a configuração do PostgreSQL de metadados não devem ser usadas como configuração de produção.
- O serviço de destino precisa estar acessível a partir do processo local ou do contêiner Airflow, conforme o modo de execução.

## Estrutura do projeto

```text
.
├── config/                  # Configuração local do Airflow e arquivo .env privado
├── dags/
│   └── weather_dag.py       # DAG weather_pipeline
├── data/                    # JSON extraído e Parquet intermediário
├── docker-compose.yaml      # Airflow, Redis e PostgreSQL de metadados
├── main.py                  # Entrada para execução local do ETL
├── notebooks/
│   └── analysis_data.ipynb  # Análise exploratória
├── pyproject.toml           # Metadados e dependências do projeto
└── src/pipeline_weather/
    ├── extract_data.py      # Extração da API e persistência do JSON
    ├── transform_data.py    # Normalização e transformação tabular
    └── load_data.py         # Conexão e carga no PostgreSQL
```
