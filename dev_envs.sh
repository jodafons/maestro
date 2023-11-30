export DOCKER_NAMESPACE='maestror'
export VIRTUALENV_NAMESPACE='maestror-env'
export MAESTRO_PATH=$PWD
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH
export LOGURU_LEVEL="DEBUG"





# Database
export DATABASE_SERVER_URL=$POSTGRES_SERVER_URL

# mail
export PGADMIN_DEFAULT_EMAIL=$POSTMAN_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$POSTMAN_SERVER_EMAIL_PASSWORD
export POSTMAN_SERVER_EMAIL_TO="jodafons@lps.ufrj.br"

# executor
export EXECUTOR_SERVER_BINDS="{'/home':'/home', '/mnt/cern_data':'/mnt/cern_data'}"



echo "=================================================================================="
echo "DATABASE_SERVER_URL     = ${DATABASE_SERVER_URL}"
echo "EXECUTOR_SERVER_BINDS   = ${EXECUTOR_SERVER_BINDS}"
echo "=================================================================================="


source $VIRTUALENV_NAMESPACE/bin/activate