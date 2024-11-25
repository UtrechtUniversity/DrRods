#!/usr/bin/irule -r irods_rule_engine_plugin-python-instance -F

# Use case:
#   When a collection contains many thousands of objects, the iRODS agent
#   that executes tasks related to an 'irm -r' icommand will run out-of-memory.
#
#   This rule is a workaround for the above issue.  It removes a configurable
#   number of data objects from a collection, and then stops.
#   Hence the memory demand can be controlled by limiting the maximum number
#   of objects to delete in one run.

#   Using multiple runs the user can empty a collection.


# Note: precondition is that user has own access to all collections 
#       and data objects
#       e.g. ichmod -M -r own rods /tempZone/home

import genquery
import re


def delete_dataobject(callback, name):
   callback.writeLine("stdout", "Removing:{}".format(name))
   return
   try:
      callback.msiDataObjUnlink("objPath={}".format(name), 0)
   except (Exception) as Error:	
      callback.writeLine("stdout", "--> failed")


def main(rule_args, callback, rei):
   DRYRUN = True
   COLLECTION = '/xxx'
   MAX_OBJECTS = 20

   obj_iter = genquery.row_iterator(
       "DATA_NAME",
       "COLL_NAME = '{}'".format(COLLECTION),
       genquery.AS_LIST,
       callback)

   changelist = []
   count = 0
   for row in obj_iter:
       count = count + 1
       if count > MAX_OBJECTS:
           break
       changelist.append(COLLECTION + '/' + row[0])
   for name in changelist:
       if DRYRUN:
          callback.writeLine("stdout","--DRYRUN--  name: {}".format(name))
       else:
          delete_dataobject(callback, name)
   callback.writeLine("stdout","{} dataobjects processed".format(str(len(changelist))))



INPUT *inp="null"
OUTPUT ruleExecOut

