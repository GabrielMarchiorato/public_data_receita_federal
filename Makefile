SHELL := /bin/bash

.PHONY: up down restart logs ps trigger-bronze trigger-silver trigger-gold airflow-shell

up:
	docker compose up -d --build

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f airflow

ps:
	docker compose ps

trigger-bronze:
	docker compose exec airflow airflow dags trigger bronze_raw_ingestion

trigger-silver:
	docker compose exec airflow airflow dags trigger silver_refined_delta

airflow-shell:
	docker compose exec airflow bash
