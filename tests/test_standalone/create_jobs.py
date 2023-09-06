import os, json
NUMBER_OF_JOBS=10
basepath = os.getcwd()
os.makedirs(basepath+'/jobs', exist_ok=True)
for sort in range(NUMBER_OF_JOBS):
    d = {'sort': sort}
    o = basepath + '/jobs/job.sort_%d.json'%(sort)
    with open(o, 'w') as f:
        json.dump(d, f)