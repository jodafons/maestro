
DOCKER_BASE=jodafons


all: build_base build_server build_executor

build_base: 
	docker build -t $(DOCKER_BASE)/orchestra:base .

build_server:
	cd servers/schedule && docker build -t $(DOCKER_BASE)/orchestra:server .

build_executor:
	cd servers/executor && docker build -t $(DOCKER_BASE)/orchestra:executor .

test:
	cd servers/database && docker-compose up -d
	python -m pytest -vv -s tests/
	cd servers/database && docker-compose down

push:
	docker push jodafons/orchestra:base
	docker push jodafons/orchestra:executor



deploy_server:
	cd servers/schedule && docker-compose up

deploy_db:
	cd servers/database && docker-compose up -d
	python scripts/create_database.py
	
down_db:
	cd servers/database && docker-compose down

#run:
#	$(DOCKER_CMD) run -v $(CUTQC_VOLUME_PATH):/volume -it $(DOCKER_BASE)/cutqc-base
clean:
	docker system prune -a
	