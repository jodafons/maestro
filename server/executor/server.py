


from flask import Flask, Response
from flask import request
from colorama import *
from colorama import init
from job import Job

import json
import uuid
import os
import socket

init(autoreset=True)

def random_id():
    new_uuid = uuid.uuid4()
    return str(new_uuid)[-12:]

server_id = random_id()

os.environ['ORCHESTRA_NODE_PORT']      = '10000' 
os.environ['ORCHESTRA_NODE_NAME']      = socket.gethostname()
os.environ['ORCHESTRA_NODE_MAX_JOBS']  = '10'



# flask server
app = Flask(__name__)

jobs = {}



def response(data, type='success', message='success'):
    status = 200
    if type == 'error':
        status = 500
    data = {'message':message, 'data':data}
    return Response(json.dumps(data, ensure_ascii=False), 
                    content_type="application/json; chartset=utf-8", status=status)

def error(message):
    return response({}, type="error", message=message)

def success(data, message='success'):
    return response(data, message=message)





@app.route('/ping', methods=['GET'])
def ping():
    hostname = os.environ['ORCHESTRA_NODE_NAME']
    return success({'server.hostname':hostname, 'server.id':server_id}, message='online')



@app.route('/create/', methods=['GET','POST'])
def create():

    req_body = request.form
    device  = req_body.get('job.device')
    command = req_body.get('job.command')
    job_id  = req_body.get('job.id')
    workarea = req_body.get('job.workarea')

    if job_id in jobs.keys():
        return error(f"Job id {job_id} exist into the queue. Its not possible to run.")

    job = Job( command, workarea, job_id, device=device, quite=False )
    jobs[job_id] = job
    jobs[int(job_id)].activate()

    return success( {'server.id' : server_id, 'job.id':job_id}, "Job created with id %d"%job_id)



@app.route('/run/<job_id>', methods=['GET'])
def run(job_id):

    if not int(job_id) in jobs.key():
        return error(f"Job id {job_id} not exist into the queue. Not possible to run")

    jobs[int(job_id)].run()
    return success({'job.id':job_id, 'job.status':status})



@app.route('/resume', methods=['GET'])
def resume():
    return success({'server.jobs': [job.summary() for _ , job in jobs.values()]})



@app.route('/status/<job_id>', methods=['GET'])
def status(job_id):

    if not int(job_id) in jobs.key():
        return error(f"Job id {job_id} not exist into the queue. Not possible to get the status")

    status = jobs[int(job_id)].status
    return success({'job.id':job_id, 'job.status':status})



@app.route('/kill/<job_id>', methods=['GET'])
def kill(job_id):
    
    if not int(job_id) in jobs.keys():
        return error(f"Job id {job_id} not exist into the queue. Not possible to kill")

    # kill the job
    jobs[int(job_id)].kill()
    return success({}, f'Kill job id {job_id}')



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ['ORCHESTRA_NODE_PORT']), debug=False)


