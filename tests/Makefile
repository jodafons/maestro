SHELL := /bin/bash
.ONESHELL:


#
# Test
#

all: server

server:
	docker compose -f test_server/docker-compose.yml up -d
	python -m pytest --full-trace -vvv test_server/test.py
	docker compose -f test_server/docker-compose.yml down
