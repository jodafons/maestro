
DOCKER_BASE=orchestra-server
SHELL := /bin/bash

all: build_base

build_venv:
	virtualenv -p python ${ORCHESTRA_ENV}
	source ${ORCHESTRA_ENV}/bin/activate
	pip install -U poetry
	poetry install 

activate_venv:
	source ${ORCHESTRA_ENV}/bin/activate


build_base: 
	docker build -t $(DOCKER_BASE)/orchestra-base .

clean:
	docker system prune -a
	