#!/usr/bin/env python

import sys
import argparse

from maestro                            import get_argparser_formatter
from maestro.main                       import args_parser as pilot_parser



def build_argparser( custom : bool=False):

    formatter_class = get_argparser_formatter(custom)

    parser    = argparse.ArgumentParser(formatter_class=formatter_class)
    mode = parser.add_subparsers(dest='mode')


    run_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    option = run_parent.add_subparsers(dest='option')
    option.add_parser("pilot", parents = pilot_parser()  ,help='Run as a pilot',formatter_class=formatter_class)
    mode.add_parser( "run", parents=[run_parent], help="",formatter_class=formatter_class)
    
    
    
    job_parent = argparse.ArgumentParser(formatter_class=formatter_class, add_help=False, )
    #option = task_parent.add_subparsers(dest='option')
    #option.add_parser("executor"   , parents = executor_parser()    ,help='',formatter_class=formatter_class)
    mode.add_parser( "job", parents=[job_parent], help="",formatter_class=formatter_class)

    return parser

def run_parser(args):
    if args.mode == "run":
        if args.option == "pilot":
            from maestro.main import run
            run(args)
    elif args.mode == "task":
        if args.option == "job":
            from maestro.jobs.main import run
            run(args)
      

def run():

    parser = build_argparser(custom=True)
    if len(sys.argv)==1:
        print(parser.print_help())
        sys.exit(1)

    args = parser.parse_args()
    run_parser(args)



if __name__ == "__main__":
  run()






















