
#
# Test
#


test:
	mkdir test_server 
	cp ${MAESTRO_PATH}/maestro/models.py test_server
	cp ${MAESTRO_PATH}/maestro/enumerations.py test_server
	cp ${MAESTRO_PATH}/servers/executor/consumer.py test_server
	cp ${MAESTRO_PATH}/schedule/schedule.py test_server
	cp ${MAESTRO_PATH}/pilot/pilot.py test_server
	cp ${MAESTRO_PATH}/maestro/__init__ test_server

	python -m pytest -vv tests/
	