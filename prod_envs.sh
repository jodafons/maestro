

export DOCKER_NAMESPACE='orchestra-server'
export VIRTUALENV_NAMESPACE=.orchestra-env


# Paths
export MAESTRO_PATH=$PWD
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH


# Host
export HOSTNAME=$(hostname).$(dnsdomainname)



#
# Ports
#
export DATABASE_SERVER_PORT=5432
export PGADMIN_SERVER_PORT=5433
export MAESTRO_WEB_SERVER_PORT=5434
export POSTMAN_SERVER_PORT=5435
export SCHEDULE_SERVER_PORT=5002
export PILOT_SERVER_PORT=5003



# Database
export DATABASE_SERVER_HOST=${POSTGRES_SERVER_HOST}


# POSTMAN
export POSTMAN_SERVER_HOST="http://maestro-server.lps.ufrj.br:${POSTMAN_SERVER_PORT}"


# Database web admin
export PGADMIN_SERVER_HOST="http://maestro-server.lps.ufrj.br:${PGADMIN_SERVER_PORT}"
export PGADMIN_DEFAULT_EMAIL=$POSTMAN_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$POSTMAN_SERVER_EMAIL_PASSWORD


# Pilot
export PILOT_SERVER_HOST="http://maestro-server.lps.ufrj.br:${PILOT_SERVER_PORT}"

# Schedule
export SCHEDULE_SERVER_HOST="http://maestro-server.lps.ufrj.br:${SCHEDULE_SERVER_PORT}"


# Executor
export EXECUTOR_AVAILABLE_PARTITIONS="cpu,cpu-large,gpu,gpu-large"
export EXECUTOR_SERVER_BINDS="{'/home':'/home', '/mnt/cern_data':'/mnt/cern_data'}"


# Web
export MAESTRO_WEB_SERVER_HOST="http://maestro-server.lps.ufrj.br:${MAESTRO_WEB_SERVER_PORT}"





echo "=================================================================================="
echo "SCHEDULE_SERVER_HOST    = ${SCHEDULE_SERVER_HOST}"
echo "PILOT_SERVER_HOST       = ${PILOT_SERVER_HOST}"
echo "DATABASE_SERVER_HOST    = ${DATABASE_SERVER_HOST}"
echo "PGADMIN_SERVER_HOST     = ${PGADMIN_SERVER_HOST}" 
echo "MAESTRO_WEB_SERVER_HOST = ${MAESTRO_WEB_SERVER_HOST}"
echo "POSTMAN_SERVER_HOST     = ${POSTMAN_SERVER_HOST}"
echo "EXECUTOR_SERVER_BINDS   = ${EXECUTOR_SERVER_BINDS}"
echo "=================================================================================="
