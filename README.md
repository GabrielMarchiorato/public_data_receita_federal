# Open Finance - Pipeline CNPJ (Medallion)

Pipeline de dados para ingestao e processamento dos Dados Publicos de CNPJ da Receita Federal, executando em ambiente containerizado com Airflow + MinIO (S3-like), seguindo arquitetura Medallion.

## Stack utilizada

  - **Orquestracao:** Apache Airflow
  - **Data Lake local (substituto do S3):** MinIO
  - **Processamento:** PySpark (preparado no ambiente)
  - **Formato analitico:** Delta Lake 
  - **Containerizacao:** Docker + Docker Compose

## Arquitetura (Medallion)
### Bronze (implementado)

- **DAG:** `bronze_raw_ingestion`
- **Fonte:** indice de arquivos ZIP da Casa dos Dados
- **Fluxo:**
  1. lista ZIPs disponiveis no indice HTML;
  2. aplica filtros configuraveis por dataset;
  3. faz download dos ZIPs em chunks;
  4. extrai arquivos;
  5. grava no MinIO em layout particionado por data de referencia.

- **Layout de objetos (Bronze):**
  - `s3://datacorp-bronze/cnpj/<dataset>/year=YYYY/month=MM/day=DD/<arquivo>`

### Silver (implementado)

- Leitura da Bronze no MinIO
- Padronizacao de schema e tipos
- Regras basicas de qualidade
- Escrita em Delta no bucket Silver

### Gold (proxima etapa)

## Decisao arquitetural

- Para datasets fracionados (ex.: `Empresas0..9`, `Estabelecimentos0..9`, `Socios0..9`), o pipeline pode ingerir apenas os primeiros arquivos (ex.: `*0.zip`) com:
  - `CNPJ_MAX_FILES_PER_FILTER=1`
- Para datasets nao fracionados (ex.: `Cnaes.zip`, `Municipios.zip`), ingere o arquivo unico.

Fiz dessa forma no intuito de reduzir o tempo de execução do projeto, possibilitando assim uma validação mais rápida, pois para testes não é necessário processar todas as bases, que juntas somam um total de 8gb aproximados.

## Estrutura do projeto
```text
public_data_receita_federal/
├── dags/
│   ├── bronze_raw_ingestion.py
│   └── silver_refined_delta.py
├── logs/
├── .env.example
├── .gitignore
├── docker-compose.yaml
├── Dockerfile
├── entrypoint.sh
├── Makefile
├── README.md
├── requirements.txt
```

## Como executar (será adicionado na documentação após finalização)