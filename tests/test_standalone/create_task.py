import os
basepath = os.getcwd()

image="/mnt/cern_data/images/torch_base_latest.sif"
command = "maestro task create -i {BASE}/jobs -t task.standalone --exec 'python {BASE}/program.py -j %IN' -p cpu --image {IMAGE}"
os.system(command.format(BASE=basepath,IMAGE=image))