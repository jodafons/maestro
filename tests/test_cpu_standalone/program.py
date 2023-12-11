import argparse
import os, sys, json
from time import sleep
from loguru import logger


def main():
    # Training settings
    parser = argparse.ArgumentParser(description = '', add_help = False)
    parser = argparse.ArgumentParser()

    parser.add_argument('-j','--job', action='store',
            dest='job', required = True,
                help = "The job config file.")

    if len(sys.argv)==1:
      parser.print_help()
      sys.exit(1)
    
    args = parser.parse_args()

    logger.info('Starting job...')

    # getting parameters from the job configuration
    job             = json.load(open(args.job, 'r'))
    seed            = job['seed']
   
    # getting parameters from the server
    device       = int(os.environ['CUDA_VISIBLE_DEVICES'])
    workarea     = os.environ['JOB_WORKAREA']
    job_id       = os.environ['JOB_ID']
    run_id       = os.environ['TRACKING_RUN_ID']
    tracking_url = os.environ['TRACKING_URL']
    dry_run      = os.environ['JOB_DRY_RUN'] == 'true'
    logger.info(run_id)
    logger.info("dry run? " + "Yes" if dry_run else "No")

    sleep(5)

    logger.info('Finish job...')
    sys.exit(0)


if __name__ == '__main__':
    main()










