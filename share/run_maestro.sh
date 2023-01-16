#!/bin/bash


MASTER=$1 # should be 0 or 1
GPUS=$2
CPUS=$3

# setup your orchestra paths here...
export ORCHESTRA_BASEPATH=/home/joao.pinto/git_repos/orchestra-server
export ORCHESTRA_VIRTUALENV=/home/joao.pinto/git_repos/orchestra-server/orchestra-env/bin

source $ORCHESTRA_VIRTUALENV/activate 
cd $ORCHESTRA_BASEPATH

# configure all staff inside of dev_envs script
source dev_envs.sh 

if [[ $MASTER -eq 1 ]];
then
    echo "Running as master node"
    maestro.py pilot run --gpus $GPUS --cpus $CPUS -m 
else
    echo "Running as slave node"
    maestro.py pilot run --gpus $GPUS --cpus $CPUS
fi
