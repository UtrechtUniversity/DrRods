#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import sys
import argparse

from drrods_common import print_stderr, open_w, csv_dquote
import drrods_sql
import icat



def same_resource_replicas(icat_connection):
    query = """
    select d.data_id, d.data_repl_num, c.coll_name, d.data_name, d.resc_id
    from r_data_main d, r_coll_main c
    where d.coll_id = c.coll_id
    order by data_id, resc_id
    """
    data_objects = []
    try:
        with icat_connection.cursor('serverside_cursor') as cur:
            cur.execute(query)
            data_id = ''
            ref = None
            problems = {}
            for row in icat_connection.iter_row(cur):
                if data_id != row[0]:
                    # start of a new data object
                    if len(problems) > 0:
                        # register problems found at previous data object
                        data_objects.append(
                                (data_id, ref[2], ref[3], list(problems.keys())))
                        problems = {}
                    # register attributes of first replica
                    # will serve as reference
                    data_id = row[0]
                    ref = row
                    continue
                # this must be the next replica of the same data object
                # register problem if replica on same resource
                if row[4] == ref[4]:
                    problems[row[4]] = True
                else:
                    ref = row
            if len(problems) > 0:
                # register problems found at last data object
                data_objects.append(
                        (data_id, ref[2], ref[3], list(problems.keys())))
    except (Exception) as error:
        print_stderr(error)
        return []
    return data_objects


def main(outputfile):
    connection = icat.Icat()
    print_stderr(connection)
    if not connection.is_connected():
        sys.exit(1)

    obj_list = same_resource_replicas(connection)
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
