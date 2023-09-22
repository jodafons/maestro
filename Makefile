

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
build_executor:
	make build_local

build_local:
	virtualenv -p python venv
	source venv/bin/activate && pip install poetry && poetry install && which python


#
# executor
#
start:
	cd servers && make start

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
	
