#!/usr/bin/env python3

try:
  # in case we have root installed here
  import ROOT
  from ROOT import gROOT
  ROOT.PyConfig.IgnoreCommandLineOptions = True
  gROOT.SetBatch()
except:
  pass


import sys, os, argparse
from orchestra.database import postgres_client
from orchestra.api import DeviceParser, PilotParser, TaskParser


parser = argparse.ArgumentParser()
commands = parser.add_subparsers(dest='mode')
db = postgres_client( os.environ['ORCHESTRA_DATABASE_HOST'] )

parsers = [
            PilotParser(db, commands),
            DeviceParser(db, commands),
            TaskParser(db, commands),
          ]



if len(sys.argv)==1:
  print(parser.print_help())
  sys.exit(1)

args = parser.parse_args()

# Run!
for p in parsers:
  p.compile(args)



























