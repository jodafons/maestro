#!/usr/bin/env python
import sys, os, argparse
from maestro.console.parsers import *

from rich_argparse import RichHelpFormatter
from rich.terminal_theme import DIMMED_MONOKAI
from rich_argparse import RichHelpFormatter
RichHelpFormatter.styles["argparse.args"]     = "blue"
RichHelpFormatter.styles["argparse.prog"]     = "grey50"
RichHelpFormatter.styles["argparse.groups"]   = "green"
RichHelpFormatter.styles["argparse.help"]     = "grey50"
RichHelpFormatter.styles["argparse.metavar"]  = "bold magenta"

def run():



  parser   = argparse.ArgumentParser(formatter_class=RichHelpFormatter)
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


























