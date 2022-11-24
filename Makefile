
DOCKER_BASE=jodafons


all: build_base build_server build_executor


# Build the container
build_base: ## Build the container
	docker build -t $(DOCKER_BASE)/orchestra:base .

build_server:
	cd servers/schedule && docker build -t $(DOCKER_BASE)/orchestra:server .

build_executor:
	cd servers/executor && docker build -t $(DOCKER_BASE)/orchestra:executor .

test:
	cd servers/database && docker-compose up -d
	python -m pytest -vv tests/
	cd servers/database && docker-compose down



deploy_server:
	cd servers/schedule && docker-compose up

deploy_db:
	cd servers/database && docker-compose up -d
	python scripts/create_database.py
	
down_db:
	cd servers/database && docker-compose down



#run:
#	$(DOCKER_CMD) run -v $(CUTQC_VOLUME_PATH):/volume -it $(DOCKER_BASE)/cutqc-base
