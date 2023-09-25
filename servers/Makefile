SHELL := /bin/bash
.ONESHELL:

all: build_postman build_executor build_schedule build_pilot

build_postman:
	cd postman
	cp -r ${MAESTRO_PATH}/maestro/api .
	cp ${MAESTRO_PATH}/maestro/enumerations.py .
	cp ${MAESTRO_PATH}/maestro/models.py .
	cp ${MAESTRO_PATH}/maestro/schemas.py .
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/postman-server --compress .
	rm -rf api 
	rm enumerations.py
	rm models.py
	rm schemas.py

build_executor:
	cd executor
	cp ${MAESTRO_PATH}/maestro/enumerations.py .
	cp ${MAESTRO_PATH}/maestro/models.py .
	cp -r ${MAESTRO_PATH}/maestro/api .
	cp ${MAESTRO_PATH}/maestro/schemas.py .
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/executor-server --compress .
	rm -rf api 
	rm enumerations.py
	rm models.py
	rm schemas.py

build_schedule:
	cd schedule
	cp ${MAESTRO_PATH}/maestro/enumerations.py .
	cp ${MAESTRO_PATH}/maestro/models.py .
	cp -r ${MAESTRO_PATH}/maestro/api .
	cp ${MAESTRO_PATH}/maestro/schemas.py .
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/schedule-server --compress .
	rm -rf api 
	rm enumerations.py
	rm models.py
	rm schemas.py

build_pilot:
	cd pilot
	cp ${MAESTRO_PATH}/maestro/enumerations.py .
	cp ${MAESTRO_PATH}/maestro/models.py .
	cp -r ${MAESTRO_PATH}/maestro/api .
	cp ${MAESTRO_PATH}/maestro/schemas.py .
	docker build --progress=plain --build-arg DOCKER_NAMESPACE=${DOCKER_NAMESPACE} -t ${DOCKER_NAMESPACE}/pilot-server --compress .
	rm -rf api 
	rm enumerations.py
	rm models.py
	rm schemas.py

#
# dev
#
dev_up:
	docker compose -f docker-compose-dev.yml up -d

dev_down:
	docker compose -f docker-compose-dev.yml down

dev_up_debug:
	make
	docker compose -f docker-compose-dev.yml up

#
# prod
#
up:
	make
	docker compose -f docker-compose.yml up

down:
	docker compose -f docker-compose.yml down

start:
	cd executor && python main.py
