
export LOCAL_ENV=orchestra-env
export MAESTRO_PATH=$PWD

export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`:$PATH
export PATH=`pwd`/scripts:$PATH

export LOCAL_HOST=$(hostname).$(dnsdomainname)


export DOCKER_NAMESPACE='orchestra-server'
# Database
export DATABASE_SERVER_HOST="postgresql://postgres:postgres@146.164.147.44:5432/postgres"
export DATABASE_SERVER_DATA_PATH=$HOME/.data


# Mailing
export MAILING_SERVER_EMAIL_FROM="cluster@lps.ufrj.br"
export MAILING_SERVER_EMAIL_PASSWORD="@LPS_Cluster#2019"
export PGADMIN_DEFAULT_EMAIL=$MAILING_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$MAILING_SERVER_EMAIL_PASSWORD

# Pilot 
export PILOT_SERVER_HOST="http://maestro-server.lps.ufrj.br:5003"

# Executor
export EXECUTOR_SERVER_HOST="http://${LOCAL_HOST}:5000"


# Web
export WEB_SERVER_HOST="http://maestro-server.lps.ufrj.br:5010"



echo "=================================================================================="
echo "EXECUTOR_SERVER_HOST = ${EXECUTOR_SERVER_HOST}"
echo "PILOT_SERVER_HOST    = ${PILOT_SERVER_HOST}"
echo "WEB_SERVER_HOST      = ${WEB_SERVER_HOST}"
echo "DATABASE_SERVER_HOST = ${DATABASE_SERVER_HOST}"
echo "=================================================================================="
