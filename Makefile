

DOCKER_NAMESPACE=orchestra-server
SHELL := /bin/bash


all: build_executor


make server:
	make build_base
	make build_mailing
	make up_debug

#
# Build
#
build_local:
	virtualenv -p python ${ORCHESTRA_ENV}
	source ${ORCHESTRA_ENV}/bin/activate && pip install poetry && poetry install && which python
build_base: 
	docker build --progress=plain -t ${DOCKER_NAMESPACE}/base-server --compress .
build_mailing:
	cd servers && make 
build_executor:
	make build_local



#
# executor
#
start_executor:
	source ${ORCHESTRA_ENV}/bin/activate && cd servers/executor && python main.py


#
# Server
#
up_debug:
	cd servers && make up_debug

up:
	cd servers && make up

down:
	cd servers && make down



#
# Docker
#

clean:
	docker system prune -a
	