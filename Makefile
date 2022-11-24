
DOCKER_BASE=jodafons


all: build_base build_server


# Build the container
build_base: ## Build the container
	docker build -t $(DOCKER_BASE)/orchestra:base .

build_server:
	cd servers/schedule
	docker build -t $(DOCKER_BASE)/orchestra:server .
	cd ../..

push:
	docker push  $(DOCKER_BASE)/orchestra-base:latest

test:
	python -m pytest -vv tests/

#run:
#	$(DOCKER_CMD) run -v $(CUTQC_VOLUME_PATH):/volume -it $(DOCKER_BASE)/cutqc-base
