
import os
basepath = os.getcwd()

command = "maestro.py task create -i {BASE}/jobs -t task.standalone --exec 'python {BASE}/program.py -j %IN' --skip_test"
os.system(command.format(BASE=basepath))