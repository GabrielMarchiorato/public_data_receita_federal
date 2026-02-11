#!/bin/bash
set -e

# Inicializa o banco de dados (Sqlite) se não existir
if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo "Inicializando o Banco de Dados do Airflow..."
  airflow db init
  
  echo "Criando usuário Admin..."
  airflow users create \
    --username admin \
    --password admin \
    --firstname Teddy \
    --lastname User \
    --role Admin \
    --email admin@example.com
fi

# Inicia o Scheduler em segundo plano
airflow scheduler &

# Inicia o Webserver em primeiro plano
exec airflow webserver