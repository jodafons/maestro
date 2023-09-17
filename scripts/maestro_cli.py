#!/usr/bin/env python


try:
  # in case we have root installed here
  import ROOT
  from ROOT import gROOT
  ROOT.PyConfig.IgnoreCommandLineOptions = True
  gROOT.SetBatch()
except:
  pass

import sys, os, argparse
from maestro.task_parser import task_parser
from maestro.database_parser import database_parser


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')


host = os.environ['DATABASE_SERVER_HOST']

parsers = [
            task_parser(host, commands),
            database_parser(host, commands ),
          ]

if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for p in parsers:
  p.parser(args)



























