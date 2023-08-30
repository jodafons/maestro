SHELL := /bin/bash
.ONESHELL:

all: build_mailing build_executor build_schedule build_pilot

build_mailing:
	cd mailing
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/mailing-server --compress .

build_executor:
	cd executor
	cp ${MAESTRO_PATH}/maestro/enumerations.py .
	cp ${MAESTRO_PATH}/maestro/models.py .
	cp -r ${MAESTRO_PATH}/maestro/api .
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/executor-server --compress .

build_schedule:
	cd schedule
	cp ${MAESTRO_PATH}/maestro/enumerations.py .
	cp ${MAESTRO_PATH}/maestro/models.py .
	cp -r ${MAESTRO_PATH}/maestro/api .
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/schedule-server --compress .

build_pilot:
	cd pilot
	cp ${MAESTRO_PATH}/maestro/enumerations.py .
	cp ${MAESTRO_PATH}/maestro/models.py .
	cp -r ${MAESTRO_PATH}/maestro/api .
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/pilot-server --compress .


up:
	docker compose -f docker-compose.yml up -d

down:
	docker compose -f docker-compose.yml down

up_debug:
	make
	docker compose -f docker-compose.yml up