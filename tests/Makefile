SHELL := /bin/bash
.ONESHELL:


#
# Test
#

all: schedule

schedule:
	docker compose -f test_schedule/docker-compose.yml up -d
	pytest --full-trace -vvv test_schedule/test_completed.py
	pytest --full-trace -vvv test_schedule/test_finalized.py
	pytest --full-trace -vvv test_schedule/test_broken.py
	pytest --full-trace -vvv test_schedule/test_kill.py
	pytest --full-trace -vvv test_schedule/test_retry.py
	docker compose -f test_schedule/docker-compose.yml down

parser:
	docker compose -f test_schedule/docker-compose.yml up -d
	pytest --full-trace -vvv test_parser/test_parser.py
	docker compose -f test_schedule/docker-compose.yml down
