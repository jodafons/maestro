__all__ = ["node_parser"]


import glob, traceback, os, argparse, re

from expand_folders import expand_folders
from tabulate import tabulate
from tqdm import tqdm
from pprint import pprint
from loguru import logger


from maestro import schemas
from maestro.servers.executor.consumer import Job as JobTest
from maestro.enumerations import JobStatus, TaskStatus, TaskTrigger, job_status
from maestro.models import Task, Job, Database, Session

pilot_host = "http://caloba91.lps.ufrj.br:5000"


def get_size(bytes, suffix="B"):
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f}{unit}{suffix}"
        bytes /= factor


def list_nodes( session: Session ):

  try:

    api = schemas.client(pilot_host, "pilot")
    answer = api.try_request("system_info" , method="get")

    if answer.status:

        nodes = {o['node']:o for o in answer.metadata.values()}
        pprint(nodes)
        table = []
        for name, node in nodes.items():

            row = [
                name, 
                node['cpu']['processor'],
                node['cpu']['cpu'],
                node['cpu']['cpu_usage'],
                get_size(node['memory']['memory_total']),
            ]
            table.append(row)
        t = tabulate(table,  headers=[
                      'Host' ,
                      'CPU'  ,
                      'CPU Cores',
                      'CPU Usage',
                      'Memory',
                      ],tablefmt="heavy_outline")
        return t

  except Exception as e:
    traceback.print_exc()
    logger.error("Unknown error." )
    return "Not possible to show the table."


class node_parser:

  def __init__(self , host, args=None):

    self.db = Database(host)
    if args:

      # Create Task
      list_parser   = argparse.ArgumentParser(description = '', add_help = False)

      parent = argparse.ArgumentParser(description = '', add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('list'  , parents=[list_parser])
      args.add_parser( 'node', parents=[parent] )

  

  def parser( self, args ):

    if args.mode == 'node':

      if args.option == 'list':
        self.list()
          
      else:
        logger.error("Option not available.")


  def list(self):
    with self.db as session:
      print(list_nodes(session))
  

  

















