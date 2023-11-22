export DOCKER_NAMESPACE='maestro'
export VIRTUALENV_NAMESPACE='maestro-env'
export MAESTRO_PATH=$PWD
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH

export LOGURU_LEVEL="INFO"


# Ports
export EXECUTOR_SERVER_PORT=5000
export PILOT_SERVER_PORT=6000

# Database
export DATABASE_SERVER_HOST=$POSTGRES_SERVER_HOST

# MLflow
export MLFLOW_SERVER_HOST="http://mlflow-server.lps.ufrj.br:5000"

# mail
export PGADMIN_DEFAULT_EMAIL=$POSTMAN_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$POSTMAN_SERVER_EMAIL_PASSWORD
export POSTMAN_SERVER_EMAIL_TO="jodafons@lps.ufrj.br"

export EXECUTOR_SERVER_BINDS="{'/home':'/home', '/mnt/cern_data':'/mnt/cern_data'}"



echo "=================================================================================="
echo "DATABASE_SERVER_HOST    = ${DATABASE_SERVER_HOST}"
echo "EXECUTOR_SERVER_BINDS   = ${EXECUTOR_SERVER_BINDS}"
echo "=================================================================================="
