

CURRENT_DIR=$PWD
REPO_DIR=$(mktemp -d)

cd $REPO_DIR
echo "download repository..."
git clone https://github.com/jodafons/orchestra-server.git && cd orchestra-server
git checkout dev

ls
make build_local

cd $CURRENT_DIR



