
MY_PLACE=$PWD
DIR=$(mktemp -d)
git clone https://github.com/jodafons/orchestra-server.git $DIR/executor
cd $DIR/executor
git checkout dev
ls
make build_local
cd $MY_PLACE

