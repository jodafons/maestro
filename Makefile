

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

#
# Server
#
start:
	cd servers && make up_prod_debug
down:
	cd servers && make down

#
# Docker
#

clean:
	docker system prune -a
	

build_local:
	virtualenv -p python ${VIRTUALENV_NAMESPACE}
	source ${MAESTRO_PATH}/${VIRTUALENV_NAMESPACE}/bin/activate && pip install poetry && poetry install && which python

