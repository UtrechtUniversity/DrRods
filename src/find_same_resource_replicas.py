#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import sys
import argparse

from drrods_common import print_stderr, open_w, csv_dquote
import drrods_sql
import icat


def main(outputfile):
    connection = icat.Icat()
    print_stderr(connection)
    if not connection.is_connected():
        sys.exit(1)

    obj_list = drrods_sql.same_resource_replicas(connection)
    print_stderr(
        '{} data ojects with multiple replicas on same resource'.
        format(len(obj_list)))
    if len(obj_list) > 0:
        with open_w(outputfile) as f:
            f.write('"data_id" ; "logical_path" ; "[attributes that vary]"\n')
            for obj in obj_list:
                (data_id, coll_name, data_name, problems) = obj
                f.write(csv_dquote(str(data_id)) + '; ' +
                        csv_dquote(coll_name + '/' + data_name) + '; ' +
                        csv_dquote(str(problems)) + '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            # formatter_class=argparse.RawDescriptionHelpFormatter,
            description='''
Finds data objects that have multiple replicas on the same resource.
Optionally writes a list of these objects to an output file (or stdout).
        ''')

    parser.add_argument(
            'outputfile', nargs='?', metavar='<outputfile>|"-"',
            help='''
output to file of a list of objects and resources used by multiple replicas''')
    cmd = parser.parse_args()
    main(cmd.outputfile)
