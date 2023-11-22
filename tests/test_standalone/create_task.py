import os
basepath = os.getcwd()

image="/mnt/cern_data/images/rxwgan_latest.sif"
command = "maestro_cli.py task create -i {BASE}/jobs -t task.standalone8 --exec 'python {BASE}/program.py -j %IN' -p gpu --image {IMAGE}"
os.system(command.format(BASE=basepath,IMAGE=image))