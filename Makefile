
DOCKER_BASE=orchestra-server


all: build_base

build_base: 
	docker build -t $(DOCKER_BASE)/orchestra-base .

clean:
	docker system prune -a
	