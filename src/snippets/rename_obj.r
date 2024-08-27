#!/usr/bin/irule -r irods_rule_engine_plugin-python-instance -F

# see iRODS issue: https://github.com/irods/irods/issues/5992

# Note: precondition is that user has own access to all collections 
#       and data objects
#       e.g. ichmod -M -r own rods /tempZone/home

import genquery
import re


def modify_collection(callback, oldname, newname):
   callback.writeLine("stdout", "OLDC:{}".format(oldname))
   callback.writeLine("stdout", "NEWC:{}".format(newname))
   callback.msiDataObjRename(oldname, newname, 1, 0)

def modify_dataobject(callback, oldname, newname):
   callback.writeLine("stdout", "OLDD:{}".format(oldname))
   callback.writeLine("stdout", "NEWD:{}".format(newname))
   callback.msiDataObjRename(oldname, newname, 0, 0)



def main(rule_args, callback, rei):
   DRYRUN = True


   # modify DATAOBJECTS 
   regex_data = re.compile(".*'.* and .*")

   obj_iter = genquery.row_iterator(
       "COLL_NAME, DATA_NAME",
       "DATA_NAME like '% and %'",
       genquery.AS_LIST,
       callback)

   changelist = []
   for row in obj_iter:
       if not regex_data.match(row[1]):
          continue
       oldname = row[0] + '/' + row[1]
       newname = row[0] + '/' + row[1].replace(" and ", "_and_")
       changelist.append((oldname, newname))

   for oldname,newname in changelist:
       if DRYRUN:
          callback.writeLine("stdout","--DRYRUN--\nOLDD:{}\nNEWD:{}\n".format(oldname, newname))
       else:
          modify_dataobject(callback, oldname, newname)
   callback.writeLine("stdout","{} dataobjects processed".format(str(len(changelist))))

   # modify COLLECTIONS
   # NB: collections are returned in reverse order, so that 
   # in case issue is located on multiple levels then the
   # lowest level is updated first.
   # (otherwise oldname might no longer exist on higher level)

   regex_coll = re.compile(".*'.* and [^/]*$")
   coll_iter = genquery.row_iterator(
       "ORDER_DESC(COLL_NAME)",
       "COLL_NAME like '% and %'",
       genquery.AS_LIST,
       callback)

   changelist = []
   for row in coll_iter:
       if not regex_coll.match(row[0]):
          continue
       oldname = row[0]
       newname = row[0].replace(" and ","_and_")
       changelist.append((oldname,newname))

   for oldname, newname in changelist:
       if DRYRUN:
          callback.writeLine("stdout","--DRYRUN--\nOLDC:{}\nNEWC:{}\n".format(oldname, newname))
       else:
          modify_collection(callback, oldname, newname)
   callback.writeLine("stdout","{} collections processed".format(str(len(changelist))))



INPUT *inp="null"
OUTPUT ruleExecOut

