SHELL := /bin/bash
.ONESHELL:


#
# Test
#

all: server

server:
	docker compose -f test_schedule/docker-compose.yml up -d
	pytest --full-trace -vvv test_schedule/test_completed.py
	pytest --full-trace -vvv test_schedule/test_finalized.py
	pytest --full-trace -vvv test_schedule/test_broken.py
	docker compose -f test_schedule/docker-compose.yml down
