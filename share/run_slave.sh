#!/bin/bash

GPUS=$1
CPUS=$2

# setup your orchestra paths here...
export ORCHESTRA_BASEPATH=/home/joao.pinto/git_repos/orchestra-server
export ORCHESTRA_VIRTUALENV=/home/joao.pinto/git_repos/orchestra-server/orchestra-env/bin

source $ORCHESTRA_VIRTUALENV/activate 
cd $ORCHESTRA_BASEPATH

# configure all staff inside of dev_envs script
source dev_envs.sh 
maestro.py pilot run --gpus $GPUS --cpus $CPUS
