
export DOCKER_NAMESPACE='orchestra-server'
export ORCHESTRA_ENV=orchestra-env

# Set all envs
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH



export MAILING_SERVER_EMAIL_FROM="cluster@lps.ufrj.br"
export MAILING_SERVER_EMAIL_PASSWORD="@LPS_Cluster#2019"
export DATABASE_PATH=/home/cluster/data
export PGADMIN_DEFAULT_EMAIL=$MAILING_SERVER_EMAIL_FROM
export PGADMIN_DEFAULT_PASSWORD=$MAILING_SERVER_EMAIL_PASSWORD


