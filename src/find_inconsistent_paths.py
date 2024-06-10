#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import sys
import argparse

from drrods_common import *
import drrods_sql
import icat

CSV_SEPARATOR = ';'

def main(outputfile):
    if not outputfile:
        outputfile = '-'
    connection = icat.Icat()
    print_stderr(connection)
    if not connection.is_connected():
        sys.exit(1)

    replicas = drrods_sql.inconsistent_paths(connection)

    print_stderr("{} replicas found".format(len(replicas)) )
    if len(replicas) == 0:
        sys.exit(0)

    with open_w(outputfile) as f:
        # CSV header
        separator = ''    # first field not preceeded by separator
        for field in drrods_sql.inconsistent_paths_columns():
            f.write(separator + csv_dquote(field))
            separator = CSV_SEPARATOR
        f.write('\n')            
        
        # CSV content lines
        for r in replicas:
            separator = ''
            for field in r:
                f.write(separator + csv_dquote(str(field)) )
                separator = CSV_SEPARATOR
            f.write('\n')  # NB: we deviate from RFC4180 which requires \r\n

    print_stderr('Output written to file ' + outputfile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description = 'Finds replicas which have a data_path that is not equivalent to the logical path. Does not report replicas with a data_path *outside* the vault.')
    parser.add_argument('outputfile', nargs='?', metavar='<outputfile>|"-"',
            help = 'output data to a csv-formatted file')
    cmd = parser.parse_args()

    main(cmd.outputfile)

