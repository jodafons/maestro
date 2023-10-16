
export DOCKER_NAMESPACE='orchestra-server'
export VIRTUALENV_NAMESPACE=.orchestra-server
export MAESTRO_PATH=$PWD
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH


export LOGURU_LEVEL="DEBUG"


#
# Ports
#
export EXECUTOR_SERVER_PORT=5000
export PILOT_SERVER_PORT=6000


# Database
export DATABASE_SERVER_HOST=$POSTGRES_SERVER_HOST


# mail
export PGADMIN_DEFAULT_EMAIL=$POSTMAN_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$POSTMAN_SERVER_EMAIL_PASSWORD



export EXECUTOR_SERVER_BINDS="{'/home':'/home', '/mnt/cern_data':'/mnt/cern_data'}"






#echo "=================================================================================="
#echo "EXECUTOR_SERVER_HOST      = ${EXECUTOR_SERVER_HOST}"
#echo "SCHEDULE_SERVER_HOST      = ${SCHEDULE_SERVER_HOST}"
#echo "PILOT_SERVER_HOST         = ${PILOT_SERVER_HOST}"
#echo "DATABASE_SERVER_HOST      = ${DATABASE_SERVER_HOST}"
#echo "PGADMIN_SERVER_HOST       = ${PGADMIN_SERVER_HOST}" 
#echo "MAESTRO_WEB_SERVER_HOST   = ${MAESTRO_WEB_SERVER_HOST}"
#echo "POSTMAN_SERVER_HOST       = ${POSTMAN_SERVER_HOST}"
#echo "=================================================================================="
#echo "EXECUTOR_SERVER_BINDS     = ${EXECUTOR_SERVER_BINDS}"
#echo "DATABASE_SERVER_DATA_PATH = ${DATABASE_SERVER_DATA_PATH}"
#echo "=================================================================================="
