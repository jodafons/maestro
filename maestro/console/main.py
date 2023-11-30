#!/usr/bin/env python
import sys, os, argparse
from maestro.console.parsers import *

def run():

  parser = argparse.ArgumentParser()
  commands = parser.add_subparsers(dest='mode')

  parsers = [
              init_parser(commands),
              task_parser(commands),
              run_parser(commands),
            ]

  if len(sys.argv)==1:
    print(parser.print_help())
    sys.exit(1)

  args = parser.parse_args()

  # Run!
  for p in parsers:
    p.parser(args)

if __name__ == "__main__":
  run()


























