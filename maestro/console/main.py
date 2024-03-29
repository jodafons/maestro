#!/usr/bin/env python
import sys, argparse
from maestro.console.parsers import *
from loguru import logger
from rich_argparse import RichHelpFormatter
from rich.terminal_theme import DIMMED_MONOKAI
from rich_argparse import RichHelpFormatter

RichHelpFormatter.styles["argparse.args"]     = "green"
RichHelpFormatter.styles["argparse.prog"]     = "bold grey50"
RichHelpFormatter.styles["argparse.groups"]   = "bold green"
RichHelpFormatter.styles["argparse.help"]     = "grey50"
RichHelpFormatter.styles["argparse.metavar"]  = "blue"


def setup_logs( level='DEBUG' ):
    """Setup and configure the logger"""
    logger.configure(extra={"server_name" : "parser"})
    logger.remove()  # Remove any old handler
    format="<green>{time}</green> | <level>{level:^12}</level> | <cyan>{extra[server_name]:^30}</cyan> | <blue>{message}</blue>"
    logger.add(
        sys.stdout,
        colorize=True,
        backtrace=True,
        diagnose=True,
        level=level,
        format=format,
    )



def run():

  setup_logs()

  parser   = argparse.ArgumentParser(formatter_class=RichHelpFormatter)
  commands = parser.add_subparsers(dest='mode')

  parsers = [
              init_parser(commands),
              task_parser(commands),
              run_parser(commands),
              slurm_parser(commands),
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


























