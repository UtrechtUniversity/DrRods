#!/bin/python
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

    obj_list = drrods_sql.objects_with_replicas_that_vary(connection)
    print_stderr(
        '{} data ojects have good replicas where attributes are not similar'.
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
Finds good replicas that belong to the same data object yet have different
attributes.
Optionally writes a list of these replicas to an output file (or stdout).
        ''')

    parser.add_argument(
            'outputfile', nargs='?', metavar='<outputfile>|"-"',
            help='''
output to file a list of objects that have varying replica attrs''')
    cmd = parser.parse_args()
    main(cmd.outputfile)
