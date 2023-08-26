
export LOCAL_ENV=orchestra-env
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH
export LOCAL_HOST=$(hostname).$(dnsdomainname)


export DOCKER_NAMESPACE='orchestra-server'
export DATABASE_SERVER_HOST="postgresql://postgres:postgres@146.164.147.44:5432/postgres"
export SCHEDULE_SERVER_HOST="0.0.0.0:5000"
export MAILING_SERVER_EMAIL_FROM="cluster@lps.ufrj.br"
export MAILING_SERVER_EMAIL_PASSWORD="@LPS_Cluster#2019"
export PGADMIN_DEFAULT_EMAIL=$MAILING_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$MAILING_SERVER_EMAIL_PASSWORD


export DATABASE_SERVER_DATA_PATH=$HOME/data

