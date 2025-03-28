__all__ = [
    "setup_logs",
    "get_argparser_formatter",
    ]

import sys, argparse

from loguru         import logger
from rich_argparse  import RichHelpFormatter




def get_argparser_formatter( standard : bool=False):
    if not standard:
        RichHelpFormatter.styles["argparse.args"]     = "green"
        RichHelpFormatter.styles["argparse.prog"]     = "bold grey50"
        RichHelpFormatter.styles["argparse.groups"]   = "bold green"
        RichHelpFormatter.styles["argparse.help"]     = "grey50"
        RichHelpFormatter.styles["argparse.metavar"]  = "blue"
        return RichHelpFormatter
    else:
        return argparse.HelpFormatter

def setup_logs( name , level, save : bool=False, color="cyan"):
    """Setup and configure the logger"""

    logger.configure(extra={"name" : name})
    logger.remove()  # Remove any old handler
    #format="<green>{time:DD-MMM-YYYY HH:mm:ss}</green> | <level>{level:^12}</level> | <cyan>{extra[slurms_name]:<30}</cyan> | <blue>{message}</blue>"
    format="<"+color+">{extra[name]:^25}</"+color+"> | <green>{time:DD-MMM-YYYY HH:mm:ss}</green> | <level>{level:^12}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <blue>{message}</blue>"
    logger.add(
        sys.stdout,
        colorize=True,
        backtrace=True,
        diagnose=True,
        level=level,
        format=format,
    )
    if save:
        output_file = name.replace(':','_').replace('-','_') + '.log'
        logger.add(output_file, 
                   rotation="30 minutes", 
                   retention=3, 
                   format=format, 
                   level=level, 
                   colorize=False)   


