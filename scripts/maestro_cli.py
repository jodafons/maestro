#!/usr/bin/env python
import sys, os, argparse
from maestro.parsers import *


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')


url = os.environ['DATABASE_SERVER_URL']

parsers = [
            task_parser(url, commands ),
            data_parser(url, commands ),
          ]

if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for p in parsers:
  p.parser(args)



























