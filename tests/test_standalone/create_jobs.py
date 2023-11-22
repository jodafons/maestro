import os, json


NUMBER_OF_JOBS=10

basepath = os.getcwd()
os.makedirs(basepath+'/jobs', exist_ok=True)

for sort in range(NUMBER_OF_JOBS):

    job = {
        'sort'            : sort,
        'seed'            : 512*(sort+1),
        'gamma'           : 0.7,
        'lr'              : 1.0,
        'epochs'          : 5,
        'batch_size'      : 64,
        'test_batch_size' : 8,
    }
    
    o = basepath + '/jobs/job.sort_%d.json'%(sort)
    with open(o, 'w') as f:
        json.dump(job, f)