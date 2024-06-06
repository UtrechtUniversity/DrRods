#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import sys
import argparse

from drrods_common import *
import drrods_sql
import icat


def main(outputfile):
    connection = icat.Icat()
    print_stderr(connection)
    if not connection.is_connected():
        sys.exit(1)

    hardlinks = drrods_sql.hardlinks(connection) 
    count = len(hardlinks)
    print_stderr('{} hard-linked data objects found'.format(count))

    if count > 0 and outputfile:
        with open_w(outputfile) as f:
            for data_id in hardlinks:
                f.write(data_id + '\n')
        print_stderr('List of data_id written to file ' + outputfile)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description = '''
Finds data objects that have more than one logical name.
Optionally writes the data_id of the hard-linked objects to an output file (or stdout).
        ''', epilog='''
Example query to inspect a data object with data_id '123456':
    iquest "ils -L ‘%s/%s’" "select COLL_NAME,DATA_NAME where DATA_ID = '123456'"
            ''')

    parser.add_argument('outputfile', nargs='?', metavar='<outputfile>|"-"',
            help='output list of data_id of hard-linked data objects to file')
    cmd = parser.parse_args()

    main(cmd.outputfile)



