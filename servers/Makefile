SHELL := /bin/bash
.ONESHELL:

all: build_mailing

build_mailing:
	cd mailing
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/mailing-server --compress .

up:
	docker compose -f docker-compose.yml up -d

down:
	docker compose -f docker-compose.yml down

up_debug:
	docker compose -f docker-compose.yml up