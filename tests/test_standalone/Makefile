

DOCKER_NAMESPACE=orchestra-server
SHELL := /bin/bash


all: build build_sif

#
# Build
#
build: 
	docker build --progress=plain -t jodafons/torch_base:latest --compress .
	
build_sif:
	singularity build --sandbox torch_base_latest.sif docker-daemon://jodafons/torch_base:latest

push:
	docker push jodafons/torch_base:latest

pull: 
	singularity pull docker://jodafons/torch_base:latest


clean:
	docker system prune -a
	


