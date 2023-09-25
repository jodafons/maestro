#!/usr/bin/env python
import sys, os, argparse
from maestro.parsers import *


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')


host = os.environ['DATABASE_SERVER_HOST']

parsers = [
            user_parser(host, commands),
            task_parser(host, commands),
            data_parser(host, commands ),
          ]

if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for p in parsers:
  p.parser(args)



























