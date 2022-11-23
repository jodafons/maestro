
DOCKER_BASE=orchestra


all: build 


# Build the container
build: ## Build the container
	docker build -t $(DOCKER_BASE)/orchestra-base .

test:
	python -m pytest -vv tests/

