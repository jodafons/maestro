
#!/bin/sh

REPO_DIR=$(mktemp -d)
CURRENT_DIR=$PWD
cd $REPO_DIR
echo "download repository..."
git clone https://github.com/jodafons/orchestra-server.git && cd orchestra-server
git checkout dev
ls -lisah
export PYTHONPATH=$PWD:$PYTHONPATH
make build_local
make start
cd $CURRENT_DIR

