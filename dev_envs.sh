
# Set all envs
export PYTHONPATH=`pwd`:$PYTHONPATH
export PATH=`pwd`/scripts:$PATH

export ORCHESTRA_DATABASE_VOLUME="$HOME/volume"
export ORCHESTRA_DATABASE_HOST="postgresql://postgres:postgres@146.164.147.44:5432/orchestra"
export ORCHESTRA_EMAIL_FROM="cluster@lps.ufrj.br"
export ORCHESTRA_EMAIL_TO="jodafons@lps.ufrj.br"
export ORCHESTRA_EMAIL_TOKEN="@LPS_Cluster#2019"


# Set all orchestra configurations
export ORCHESTRA_BASEPATH=$PWD


#source orchestra-env/bin/activate
