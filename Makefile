help:
	@echo 'Makefile for managing docker-compose airflow                       '
	@echo '                                                                   '
	@echo 'Usage:                                                             '
	@echo ' make init       initialise environment, AWS_ACCESS_KEY, SECRET_KEY, & BUCKET_NAME '
	@echo ' make build      build images                                '
	@echo ' make up         creates containers and starts service       '
	@echo ' make down       stops service and removes containers        '

init:
	@read -p "Enter AWS_ACCESS_KEY: " AWS_ACCESS_KEY; \
	read -p "Enter AWS_SECRET_KEY: " AWS_SECRET_KEY; \
	read -p "Enter AWS_BUCKET_NAME: " AWS_BUCKET_NAME; \
	echo "AIRFLOW_UID=$$(id -u)\nAIRFLOW_GID=0\nAWS_ACCESS_KEY=$$AWS_ACCESS_KEY\nAWS_SECRET_KEY=$$AWS_SECRET_KEY\nAWS_BUCKET_NAME=$$AWS_BUCKET_NAME" > .env

build:
	docker-compose up airflow-init
	docker build -t apache/airflow:2.1.0 .

up:
	docker-compose up -d

down:
	docker-compose down
