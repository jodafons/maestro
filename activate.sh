
export VIRTUALENV_NAMESPACE='maestro-env'
export LOGURU_LEVEL="DEBUG"
export VIRTUALENV_PATH=$PWD/$VIRTUALENV_NAMESPACE
export MAESTRO_PATH=$PWD
export STORAGE_PATH="$HOME/data"



#
# DB variables
#
export DB_PATH=$STORAGE_PATH/db
#export DB_HOST=146.164.147.42
#export DB_PROTOCOL=postgresql
#export DB_IMAGE=postgres
#export DB_TYPE=postgres
#export DB_PASSWORD=postgres
#export DB_DATABASE=joao.pinto
#export DB_USER=
#export DB_PORT=5432
#export DB_PG4_PORT=5433
#export DB_STRING="$DB_PROTOCOL://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_DATABASE"
export DB_STRING="sqlite:///${DB_PATH}/maestro.db"



#
# Maestro variables
#
export MESSAGE_LEVEL="INFO"
export MAESTRO_SINGULARITY_BINDS="{'/home':'/home', '/mnt/cern_data':'/mnt/cern_data'}"
export MAESTRO_STORAGE_PATH=$STORAGE_PATH
export MAESTRO_PORT=7000
export MAESTRO_HOST="http://$localhost.lps.ufrj.br:${MAESTRO_PORT}" 
export MAESTRO_IMAGES_PATH=$STORAGE_PATH/data/images


mkdir -p $DB_PATH
mkdir -p $MAESTRO_IMAGES_PATH


#export SLURM_INCLUDE_DIR=/mnt/market_place/slurm_build/build/include
export SLURM_LIB_DIR=/usr/lib

if [ -d "$VIRTUALENV_PATH" ]; then
    echo "$VIRTUALENV_PATH exists."
    source $VIRTUALENV_PATH/bin/activate
else
    virtualenv -p python ${VIRTUALENV_PATH}
    source $VIRTUALENV_PATH/bin/activate
    pip install -e .
fi


