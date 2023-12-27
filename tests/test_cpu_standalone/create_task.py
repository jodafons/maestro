import os
basepath = os.getcwd()
virtualenv = os.environ['VIRTUALENV_PATH']

def create_task ( taskname ):
    command = f"maestro task create -i {basepath}/jobs -t {taskname} --exec 'python {basepath}/program.py -j %IN' -p cpu --virtualenv {virtualenv}"
    os.system(command)


create_task('test.3')