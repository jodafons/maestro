#!/usr/bin/env python
import sys, os, argparse
from maestro.parsers import *


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')

parsers = [
            task_parser(commands),
            data_parser(commands),
            run_parser(commands),
          ]

if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for p in parsers:
  p.parser(args)



























