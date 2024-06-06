#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import sys
import pathdb
from drrods_common import *



def resource_vault_dirs_for_host(icat_connection, hostname):
    query = """
select resc_def_path from r_resc_main
where resc_net = %s
        """
    vault_dirs = []
    try:
        with icat_connection.cursor() as cur:
            cur.execute(query, (hostname,))
            for row in icat_connection.iter_row(cur):
                vault_dirs.append(row[0])
    except(Exception) as error:
        print_stderr(error)
        return None

    return vault_dirs

def replica_paths_for_host(icat_connection, hostname):
    query = """
select data_path from r_data_main d, r_resc_main r
where d.resc_id = r.resc_id
and r.resc_net = %s
        """
    db = pathdb.Pathdb()
    replica_count = 0
    try:
        with icat_connection.cursor() as cur:
            cur.execute(query, (hostname,))
            for row in icat_connection.iter_row(cur):
                db.register(row[0])
                replica_count = replica_count + 1
    except(Exception) as error:
        print_stderr(error)
        return (0, None)
        
    return (replica_count, db)

def hardlinks(icat_connection):
    query = """
select distinct data_id,coll_id,data_name from r_data_main
        """
    db = {}
    linked = {}
    try:
        with icat_connection.cursor() as cur:
            cur.execute(query)
            for row in icat_connection.iter_row(cur):
                data_id = row[0] 
                if data_id in db:
                    (coll, dname) = db[data_id]
                    if coll == row[1] and dname == row[2]:
                        continue
                    # hardlink found
                    linked[data_id] = True
                else:
                    # register new data id
                    db[data_id] = (row[1], row[2])

    except(Exception) as error:
        print_stderr(error)
        return []

    return list(linked.keys())


def find_replicas_by_data_path(icat_connection, hostname, data_path):
    query = """
select d.data_id, c.coll_name, d.data_name, d.data_repl_num, 
d.data_is_dirty, d.create_ts, d.modify_ts
from r_data_main d, r_coll_main c, r_resc_main r
where d.coll_id = c.coll_id
and d.resc_id = r.resc_id
and r.resc_net = %s
and d.data_path = %s
        """
    replicas = []
    try:
        with icat_connection.cursor() as cur:
            cur.execute(query, (hostname, data_path,))
            for row in icat_connection.iter_row(cur):
                replicas.append(row)

    except(Exception) as error:
        print_stderr(error)
        return []

    return replicas

def count_replicas(icat_connection, data_id):
    query = """
select count(*)
from r_data_main
where data_id = %s
        """
    replica_count = None
    try:
        with icat_connection.cursor() as cur:
            cur.execute(query, (data_id,))
            row = cur.fetchone()
            replica_count = row[0]
    except(Exception) as error:
        print_stderr(error)
        return 0

    return replica_count





if __name__ == "__main__":
    pass
