

DOCKER_NAMESPACE=orchestra-server
SHELL := /bin/bash


all: build_local

#
# Build
#
build_base: 
	docker build --progress=plain -t ${DOCKER_NAMESPACE}/maestro --compress .
	
build_local:
	virtualenv -p python ${VIRTUALENV_NAMESPACE}
	source ${VIRTUALENV_NAMESPACE}/bin/activate && pip install poetry && poetry install && which python

#
# Server
#
node:
	source ${MAESTRO_PATH}/${VIRTUALENV_NAMESPACE}/bin/activate && python maestro/servers/executor/main.py

server:
	source ${MAESTRO_PATH}/${VIRTUALENV_NAMESPACE}/bin/activate && python maestro/servers/control/main.py

deploy_up:
	docker compose up -d

deploy_down:
	docker compose down

clean:
	docker system prune -a
	
poetry_to_requirements:
	poetry export --without-hashes --format=requirements.txt > requirements.txt


