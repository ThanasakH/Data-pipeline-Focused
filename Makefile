help:
	@echo 'Makefile for managing docker-compose airflow                       '
	@echo '                                                                   '
	@echo 'Usage:                                                             '
	@echo ' make init       initialise environment, AWS_ACCESS_KEY and AWS_SECRET_KEY '
	@echo ' make build      build images                                '
	@echo ' make up         creates containers and starts service       '
	@echo ' make down       stops service and removes containers        '

init:
	@$(eval ACCESS_KEY=$(shell stty -echo; read -p "AWS_ACCESS_KEY: " pwd; stty echo; echo $$pwd)) 
	@$(eval SECRET_KEY=$(shell stty -echo; read -p "AWS_SECRET_KEY: " pwd; stty echo; echo $$pwd)) 
	echo "AIRFLOW_UID=$(shell id -u)\nAIRFLOW_GID=0\nAWS_ACCESS_KEY=$(ACCESS_KEY)\nAWS_SECRET_KEY=$(SECRET_KEY)" > .env

build:
	docker-compose build
	docker-compose up airflow-init

up:
	docker-compose up -d

down:
	docker-compose down
