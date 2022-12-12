
__all__ = ["DeviceParser"]

import argparse
from orchestra.database.models import Device
from prettytable import PrettyTable


class DeviceParser:

  def __init__(self, db, args=None):

    self.__db = db
    if args:

      create_parser = argparse.ArgumentParser(description = 'Device create command lines.' , add_help = False)
      create_parser.add_argument('-n', '--hostname', action='store', dest='hostname', required=True,
                                  help = "The name of the device.")
      create_parser.add_argument('-d','--device', action='store', dest='device', type=int, default=-1,
                                  help = "The device GPU number.")
      create_parser.add_argument('-s','--slots', action='store', dest='slots', type=int, default=1,
                                  help = "The number of slots for this device")
      create_parser.add_argument('-e','--enabled', action='store', dest='enabled', type=int, default=1,
                                  help = "The number of enabled slots for this device")

  

      # Delete dataset using the dataset CLI
      list_parser = argparse.ArgumentParser(description = 'List all devices.', add_help = False)

      clear_parser = argparse.ArgumentParser(description = 'Clear all devices.', add_help = False)

      parent = argparse.ArgumentParser(description = '',add_help = False)
      subparser = parent.add_subparsers(dest='option')

      # Datasets
      subparser.add_parser('list', parents=[list_parser])
      subparser.add_parser('clear', parents=[clear_parser])

      args.add_parser( 'device', parents=[parent] )



  def compile( self, args ):
    if args.mode == 'device':
      if args.option == 'list':
        _, message  = self.list()
        print(message)
      elif args.option == 'clear':
        _, message = self.clear()
        print(message)
      else:
        print( "Not valid option.")



  #
  # List datasets
  #
  def list( self ):

    t = PrettyTable([
                      'Node'     ,
                      'Device'   ,
                      'Type'     ,
                      'Slots'    ,
                      'Status'   , 
                      ])

    # Loop over all datasets inside of the username
    for device in self.__db.devices():

      t.add_row(  [device.host,
                   device.gpu,
                   'gpu' if device.gpu>=0 else 'cpu',
                   '%d/%d'%(device.enabled, device.slots),
                   'online' if device.is_alive() else 'offline',
                  ] )

    return (True, t)



  #
  # create a node
  #
  def create( self, hostname, device, slots, enabled):

    device = Device(host=hostname, enabled=enabled, slots=slots, gpu=device)
    self.__db.session().add(device)
    self.__db.commit()
    return (True, "Successfully created." )


  #
  # Clear all devices
  #
  def clear( self ):

    # remove all device for this host
    self.__db.session().query(Device).delete()
    self.__db.commit()
    return (True, "Successfully created." )