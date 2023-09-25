
#!/bin/sh

REPO_DIR=$(mktemp -d)
CURRENT_DIR=$PWD
cd $REPO_DIR
echo "download repository..."
git clone https://github.com/jodafons/orchestra-server.git && cd orchestra-server

export VIRTUALENV_NAMESPACE=orchestra-server-env
export MAESTRO_PATH=$PWD
export PYTHONPATH=$PWD:$PYTHONPATH
ls -lisah
make build_local
make start
cd $CURRENT_DIR

