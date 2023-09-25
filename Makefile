

DOCKER_NAMESPACE=orchestra-server
SHELL := /bin/bash


all: build_base build_server

#
# Build
#
build_base: 
	docker build --progress=plain -t ${DOCKER_NAMESPACE}/base-server --compress .
build_server:
	cd servers && make 
build_local:
	virtualenv -p python ${VIRTUALENV_NAMESPACE}
	source ${MAESTRO_PATH}/${VIRTUALENV_NAMESPACE}/bin/activate && pip install poetry && poetry install && which python

#
# Server
#
up:
	cd servers && make up_debug
down:
	cd servers && make down
start:
	cd servers && make start

#
# Docker
#

clean:
	docker system prune -a
	


