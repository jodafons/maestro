export DOCKER_NAMESPACE='maestro'
export VIRTUALENV_NAMESPACE='maestro-env'
export MAESTRO_PATH=$PWD
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH


# pilot
export LOGURU_LEVEL="DEBUG"
export PILOT_SERVER_PORT=5000

# Database
export DATABASE_SERVER_RECREATE="recreate"
export DATABASE_SERVER_HOST=$POSTGRES_SERVER_HOST


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
export EXECUTOR_SERVER_SLOTS_SIZE="1"
export EXECUTOR_PARTITION="gpu"


export EXECUTOR_SERVER_WANDB_TOKEN="c58957e71528fbacfebec5e6e14a7fca8c35bffb"


echo "=================================================================================="
echo "DATABASE_SERVER_HOST    = ${DATABASE_SERVER_HOST}"
echo "EXECUTOR_SERVER_BINDS   = ${EXECUTOR_SERVER_BINDS}"
echo "=================================================================================="


source $VIRTUALENV_NAMESPACE/bin/activate