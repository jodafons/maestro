DOCKER_CMD=docker


all: build_base build_server



build_base:

	$(DOCKER_CMD) build -t $(DOCKER_BASE)/cutqc-base --compress .


build_server:

	cp -r workloads server

	cd server && $(DOCKER_CMD) build --build-arg DOCKER_BASE=$(DOCKER_BASE) -t $(DOCKER_BASE)/cutqc-server --compress .

	rm -rf server/workloads

	
#build_test:

#	cp -r server tests

#	$(DOCKER_CMD) build --build-arg DOCKER_BASE=$(DOCKER_BASE) -t $(DOCKER_BASE)/cutqc-test --compress ./tests

#	rm -rf tests/server


push:

	$(DOCKER_CMD) push $(DOCKER_BASE)/cutqc-base

	$(DOCKER_CMD) push $(DOCKER_BASE)/cutqc-server



run_base:

	$(DOCKER_CMD) run -p $(CUTQC_SERVER_PORT):$(CUTQC_SERVER_PORT) -v $(CUTQC_VOLUME_PATH):/volume -it $(DOCKER_BASE)/cutqc-base


run_test:

	$(DOCKER_CMD) run -p $(CUTQC_SERVER_PORT):$(CUTQC_SERVER_PORT) -v $(CUTQC_VOLUME_PATH):/volume -it $(DOCKER_BASE)/cutqc-test



run:

	$(DOCKER_CMD) run -p $(CUTQC_SERVER_PORT):$(CUTQC_SERVER_PORT) -v $(CUTQC_VOLUME_PATH):/volume -it $(DOCKER_BASE)/cutqc-server


run_no_volume:

	$(DOCKER_CMD) run -p $(CUTQC_SERVER_PORT):$(CUTQC_SERVER_PORT) -it $(DOCKER_BASE)/cutqc-server




deploy:

	docker-compose down

	docker-compose up -d


shutdown:

	docker-compose down



test:

	docker-compose down

	docker-compose up -d

	sleep 2

	#python -m pytest -v tests/commons/

	python -m pytest -v tests/api/

	docker-compose down



