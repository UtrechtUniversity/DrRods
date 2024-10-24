#!/usr/bin/irule -r irods_rule_engine_plugin-python-instance -F
# 2024 by Ton Smeele, Utrecht University
#
# This script reports all intermediate replicas in the zone
# 
# To ensure that selected repicas are at rest, filters any replicas 
# recently modified (last 7 days, configurable)
# 

# DATA_REPL_STATUS:
# 0 = stale, 1 = good, 2 = intermediate, 3 = read-locked, 4 = write-locked 


import genquery
import time

def irods_time(i):
    return "{:011d}".format(int(i))

def bash_escape(s):
    quoted_quote = "'" + '"' + "'" + '"' + "'"
    s = s.replace("'", quoted_quote)
    return "'" + s + "'"

def main(rule_args, callback, rei):
    day = 24 * 60 * 60
    now = time.time()
    minimum_days_ago = 7   # modify as deemed needed
    threshold_time = now - (minimum_days_ago * day) 
       

    repl_iter = genquery.row_iterator(
    "COLL_NAME, DATA_NAME, DATA_REPL_NUM, DATA_REPL_STATUS",
    "DATA_REPL_STATUS >= '2' AND DATA_MODIFY_TIME < '{}'".format(irods_time(threshold_time)),
    genquery.AS_LIST,
    callback)

    for row in repl_iter:
        path = "{}/{}".format(row[0], row[1])
        repl = row[2]
        status = row[3]
        #callback.writeLine("stdout", "{} -> {} {}".format(status, path, repl))
        callback.writeLine("stdout", "iadmin modrepl logical_path {} replica_number {} DATA_REPL_STATUS 0".format(bash_escape(path), repl))


INPUT *inp=foo
OUTPUT ruleExecOut 
