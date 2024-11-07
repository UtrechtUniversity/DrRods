#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

# Use this skeleton program to execute arbitrary sql queries
# to select and report data objects.  Adapt the query to your needs.

import sys
import argparse

from drrods_common import print_stderr, open_w, csv_dquote
import drrods_sql
import icat

def data_objects_selection(icat_connection):
    query = """
select data_id from r_data_main
group by data_id
having count(*) < 2
"""
    ids = []
    try:
       with icat_connection.cursor('server_side_cursor') as cur:
          cur.execute(query)
          for row in icat_connection.iter_row(cur):
             ids.append(row[0])
    except (Exception) as error:
        print_stderr(error)
        return ids

    return ids

def main():
    connection = icat.Icat()
    print_stderr(connection)
    if not connection.is_connected():
        sys.exit(1)

    ids = data_objects_selection(connection)
    for id in ids:
        print(id)        


if __name__ == "__main__":
    main()

