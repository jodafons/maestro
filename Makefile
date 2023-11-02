

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
	source ${MAESTRO_PATH}/${VIRTUALENV_NAMESPACE}/bin/activate && pip install poetry && poetry install && which python

#
# Server
#
node:
	source ${MAESTRO_PATH}/${VIRTUALENV_NAMESPACE}/bin/activate && python maestro/servers/executor/main.py

server:
	source ${MAESTRO_PATH}/${VIRTUALENV_NAMESPACE}/bin/activate && python maestro/servers/control/main.py

deploy_up:
	make build_base
	docker compose -f docker-compose-dev.yml up

deploy_down:
	docker compose -f docker-compose-dev.yml down

clean:
	docker system prune -a
	


