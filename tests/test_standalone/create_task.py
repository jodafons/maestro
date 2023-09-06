import os
basepath = os.getcwd()

command = "maestro_cli.py task create -i {BASE}/jobs -t task.standalone --exec 'python {BASE}/program.py -j %IN' -e {email}"
os.system(command.format(BASE=basepath, email='jodafons@lps.ufrj.br'))