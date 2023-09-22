


export PILOT_SERVER_HOST="http://maestro-server.lps.ufrj.br:5003"
export EXECUTOR_SERVER_HOST="http://${HOSTNAME}:5000"
export EXECUTOR_SERVER_BINDS="{'/home':'/home', '/mnt/cern_data':'/mnt/cern_data'}"
export EXECUTOR_SERVER_DEVICE="-1"
export EXECUTOR_SERVER_SLOT_SIZE="1"
export EXECUTOR_PARTITION="cpu"


REPO_DIR=$(mktemp -d)

CURRENT_DIR=$PWD
cd $REPO_DIR
echo "download repository..."
git clone https://github.com/jodafons/orchestra-server.git && cd orchestra-server
git checkout dev
source envs.sh
make build_local
make start


cd $CURRENT_DIR

