export DOCKER_NAMESPACE='maestro'
export VIRTUALENV_NAMESPACE='maestro-env'
export MAESTRO_PATH=$PWD
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH
export LOGURU_LEVEL="DEBUG"




# pilot
export PILOT_SERVER_PORT=5000

# tracking
export TRACKING_SERVER_PORT=4000
export TRACKING_SERVER_PATH=$PWD/tracking

# Database
export DATABASE_SERVER_RECREATE="recreate"
export DATABASE_SERVER_URL=$POSTGRES_SERVER_URL


# mail
export PGADMIN_DEFAULT_EMAIL=$POSTMAN_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$POSTMAN_SERVER_EMAIL_PASSWORD
export POSTMAN_SERVER_EMAIL_TO="jodafons@lps.ufrj.br"

# executor
export EXECUTOR_SERVER_PORT=5001
export EXECUTOR_SERVER_BINDS="{'/home':'/home', '/mnt/cern_data':'/mnt/cern_data'}"
export EXECUTOR_SERVER_DEVICE="0"
export EXECUTOR_SERVER_MAX_RETRY="5"
export EXECUTOR_SERVER_TIMEOUT="60" # seconds
export EXECUTOR_PARTITION="gpu"


echo "=================================================================================="
echo "DATABASE_SERVER_URL     = ${DATABASE_SERVER_URL}"
echo "EXECUTOR_SERVER_BINDS   = ${EXECUTOR_SERVER_BINDS}"
echo "=================================================================================="


source $VIRTUALENV_NAMESPACE/bin/activate