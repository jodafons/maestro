
import argparse
import sys
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
    
    #args = parser.parse_args()


    logger.info('Starting job...')


    logger.info('Finish job...')
    sys.exit(0)