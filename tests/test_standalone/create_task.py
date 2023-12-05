import os
basepath = os.getcwd()

#image="/mnt/cern_data/images/torch_base_latest.sif"
##command = "maestro task create -i {BASE}/jobs -t task.standalone --exec 'python {BASE}/program.py -j %IN' -p cpu --image {IMAGE}"
#os.system(command.format(BASE=basepath,IMAGE=image))


image="/mnt/cern_data/images/torch_base_latest.sif"
command = "maestro task create -i {BASE}/jobs -t task.standalone --exec 'python {BASE}/test_local.py -j %IN' -p cpu --virtualenv /home/joao_pinto/my_github/orchestra-server/maestro-env"
os.system(command.format(BASE=basepath,IMAGE=image))