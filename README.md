# Open Finance - Pipeline CNPJ (Medallion)

Pipeline de dados para ingestao e processamento dos Dados Publicos de CNPJ da Receita Federal, executando em ambiente containerizado com Airflow + MinIO (S3-like), seguindo arquitetura Medallion.

## Stack utilizada

  - **Orquestracao:** Apache Airflow
  - **Data Lake local (substituto do S3):** MinIO
  - **Processamento:** PySpark (preparado no ambiente)
  - **Formato analitico:** Delta Lake 
  - **Containerizacao:** Docker + Docker Compose
  - **Automacao local:** Makefile

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

### Silver (implementado)

- Leitura da Bronze no MinIO
- Padronizacao de schema e tipos
- Regras basicas de qualidade
- Escrita em Delta no bucket Silver

### Gold (proxima etapa)

- Modelo de entrega de valor com agregacao de negocio:
  - `empresas_ativas_por_cnae_uf`
- Seguindo o exemplo da pergunta feita na documentação do case:
  - distribuicao de empresas ativas por CNAE e UF.

## Decisao arquitetural

- Para datasets fracionados (ex.: `Empresas0..9`, `Estabelecimentos0..9`, `Socios0..9`), o pipeline pode ingerir apenas os primeiros arquivos (ex.: `*0.zip`) com:
  - `CNPJ_MAX_FILES_PER_FILTER=1`
- Para datasets nao fracionados (ex.: `Cnaes.zip`, `Municipios.zip`), ingere o arquivo unico.

Fiz dessa forma no intuito de reduzir o tempo de execução do projeto, possibilitando assim uma validação mais rápida, pois para testes não é necessário processar todas as bases, que juntas somam um total de 8gb aproximados. Sei que era opcional, mas achei interessante processar todos os sets de dados.

## Estrutura do projeto
```text
public_data_receita_federal/
├── dags/
│   ├── bronze_raw_ingestion.py
│   ├── gold_business_view.py
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

## Como executar

### 1) Pre-requisitos

- Docker + Docker Compose
- `make` para usar atalhos do `Makefile`

### 2) Configurar variaveis de ambiente

Crie seu `.env` a partir do template:

```bash
cp .env.example .env
```

No PowerShell (Windows):

```powershell
Copy-Item .env.example .env
```

### 3) Subir ambiente limpo

Se quiser um deploy totalmente limpo (remove banco do Airflow e dados locais do MinIO):

```bash
docker compose down -v
```

Depois suba novamente:

Com Makefile:

```bash
make up
```

Sem Makefile:

```bash
docker compose up -d --build
```

### 4) Validar que os containers subiram

Com Makefile:

```bash
make ps
```

### 5) Acessar servicos

- **Airflow UI:** http://localhost:8080  
  Usuario: `admin`  
  Senha: `admin`

- **MinIO Console:** http://localhost:9001  
  Usuario: `minioadmin`  
  Senha: `minioadmin`

### 6) Executar a DAG Bronze

Com Makefile:

```bash
make trigger-bronze
```

Ou pela UI do Airflow, executando a DAG `bronze_raw_ingestion`.

Ponto de atenção: existe a chance da execução travar com o comando do make se a DAG não estiver ativa no Airflow, ative ela caso isso aconteça

### 7) Executar a DAG Silver

Com Makefile:

```bash
make trigger-silver
```

Ou pela UI do Airflow, executando a DAG `silver_refined_delta`.

Ponto de atenção: existe a chance da execução travar com o comando do make se a DAG não estiver ativa no Airflow, ative ela caso isso aconteça

### 8) Executar a DAG Gold

Com Makefile:

```bash
make trigger-gold
```

Ou pela UI do Airflow, executando a DAG `gold_business_view`.

Ponto de atenção: existe a chance da execução travar com o comando do make se a DAG não estiver ativa no Airflow, ative ela caso isso aconteça

### 9) Acompanhar logs

Com Makefile:

```bash
make logs
```

## Fonte de dados

- Dados Abertos CNPJ (Casa dos Dados):  
  <https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/2026-01-11/>

## Boas praticas aplicadas

- Parametrizacao por variaveis de ambiente
- Idempotencia na criacao de bucket
- Download em chunks e upload em streaming para reduzir uso de memoria
- Processamento Silver isolado por tabela (uma task por dataset)
- Tratamento de erro com falha explicita quando nenhum ZIP casa com os filtros
- Separacao clara de responsabilidades por task na DAG

## Qualidade e observabilidade

- Logs por task no Airflow (`./logs`)
- Validacao basica de entrada:
  - erro se nenhum arquivo for selecionado;
  - erro se ZIP vier sem membros validos;
  - ignora membros vazios durante ingestao.

## Observações importantes

- Se necessario, aumente memoria do Spark:
```env
SPARK_DRIVER_MEMORY=3g
SPARK_EXECUTOR_MEMORY=3g
```

- A execução média das DAGS é essa:

  - Bronze: 7 minutos
  - Silver: 14 minutos
  - Gold: 2 minutos

- Os dados no MinIO devem estar assim:

  - Bronze (`datacorp-bronze`):

    ```text
    cnpj/<dataset>/year=2026/month=01/day=11/
    ```

  - Silver (`datacorp-silver`):

    ```text
    cnpj/<tabela>/
    ```
  
  - Gold (`datacorp-gold`):

    ```text
    cnpj/empresas_ativas_por_cnae_uf/
    ```

- Durante o processo de desenvolvimento tentei aplicar uma camada adicional ao projeto construindo um notebook em Python, mas devido a limitação do Windows (OS que uso), ficou muito complicada a execução e desenvolvimento por conta do tempo. O objetivo desse notebook era criar uma camada visual dos dados gerados pela camada Bronze, Silver e Gold, gerando assim uma facilidade de validação dos dados importados e tratados.