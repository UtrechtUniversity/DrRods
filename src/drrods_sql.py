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
        with icat_connection.cursor('server_side_cursor') as cur:
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
        with icat_connection.cursor('server_side_cursor') as cur:
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


def inconsistent_paths_columns():
    return [
        # columns from query inconsistent_paths:
        'hostname','data_id','data_repl_num','data_size','data_is_dirty',
        'data_checksum','modify_ts','resc_name','actual_data_path','expected_data_path',
        # columns from count_other_replicas_with_same_data_path:
        'actual_linked_use','expected_linked_use',
        # extra column that can be used to indicate processing status
        # initial values will be "NO"
        'processed'
        ]

def inconsistent_paths(icat_connection, data_id = None):
    conditional = ''
    if data_id:
        conditional = ' and d.data_id = %s '
    query = """
select replicas.resc_net, data_id, data_repl_num, data_size, data_is_dirty, 
    replicas.data_checksum as DATA_CHECKSUM, modify_ts, 
    replicas.resc_name as RESC_NAME, 
    datapath as ACTUAL,  
    vaultdir||collname||'/'||dataname as EXPECTED 
from (
   select 
      r.resc_net as RESC_NET,
      d.data_id as DATA_ID,
      d.data_repl_num as DATA_REPL_NUM,
      d.data_size as DATA_SIZE,
      d.data_is_dirty as DATA_IS_DIRTY,
      d.data_checksum as DATA_CHECKSUM,
      d.modify_ts as MODIFY_TS,
      -- collname without the zone prefix
      substr(c.coll_name,strpos(substr(c.coll_name,2),'/') + 1) as COLLNAME,
      d.data_name as DATANAME,
      d.data_path as DATAPATH,
      r.resc_name as RESC_NAME,
      r.resc_id as RESC_ID,
      r.resc_def_path as VAULTDIR,
      -- replica path without the vault dir prefix and without the basename
      substr(d.data_path, 
         length(r.resc_def_path)+1,  
         length(d.data_path) - length(r.resc_def_path) - 1 
           - position('/' in reverse(d.data_path)) + 1 ) as DIRNAME,
      substr(d.data_path, 
         length(d.data_path) - position('/' in reverse(d.data_path)) + 2 ) as BASENAME
   from r_data_main d, r_coll_main c, r_resc_main r 
   where 
      d.resc_id = r.resc_id and d.coll_id = c.coll_id
      -- filter out s3 and other resources where directory and coll_name are not kept in sync
      and r.resc_type_name like 'unix%file%system'
      -- filter out replicas with data_paths outside the vault
      and starts_with(d.data_path, r.resc_def_path)
      -- optionally limit results using extra condition 
      {} -- extra condition
   ) as replicas
 where replicas.COLLNAME <> replicas.DIRNAME 
   or replicas.DATANAME <> replicas.BASENAME
    """.format(conditional)

    replicas = []
    try:
        with icat_connection.cursor('server_side_cursor') as cur:
            if data_id:
                cur.execute(query, (data_id,))
            else :
                cur.execute(query)
            for row in icat_connection.iter_row(cur):
                actual_count = count_other_replicas_with_same_data_path(
                   icat_connection,
                   row[1],  # data_id
                   row[2],  # data_repl_num
                   row[8])  # 'actual' data_path
                expected_count = count_other_replicas_with_same_data_path(
                   icat_connection,
                   row[1],  # data_id
                   row[2],  # data_repl_num
                   row[9])  # 'expected' data_path
                processed = "NO"
                out = row + ( actual_count > 0, expected_count > 0, processed)
                replicas.append(out)

    except(Exception) as error:
        print_stderr(error)
        return []

    return replicas


def count_other_replicas_with_same_data_path(icat_connection, data_id, data_repl_num, data_path):
    query = """
    select count(*) from r_data_main d, r_resc_main r
    where (d.data_id <> %s or d.data_repl_num <> %s)
    and d.resc_id = r.resc_id
    and d.data_path = %s
    -- limit results to same host
    and r.resc_net = (
       select rs.resc_net 
       from r_resc_main rs, r_data_main ds 
       where rs.resc_id = ds.resc_id 
       and ds.data_id = %s
       and ds.data_repl_num = %s
       ) 
    """

    replica_count = None
    try:
        with icat_connection.cursor() as cur:
            cur.execute(query, (data_id, data_repl_num, data_path, data_id, data_repl_num))
            row = cur.fetchone()
            replica_count = row[0]
    except(Exception) as error:
        print_stderr(error)
        return 0

    return replica_count


def objects_with_replicas_that_vary(icat_connection):
    query = """
    select d.data_id, d.data_repl_num, d.data_name, c.coll_name, 
    d.data_type_name, d.data_size, d.data_owner_name, d.data_owner_zone
    from r_data_main d, r_coll_main c
    where d.coll_id = c.coll_id
    and d.data_is_dirty = '1'
    order by data_id
    """
    data_objects = []
    try:
        with icat_connection.cursor('serverside_cursor') as cur:
            cur.execute(query)
            data_id = ''
            ref = None
            problems = {}
            for row in icat_connection.iter_row(cur):
                if data_id != row[0] :
                    # start of a new data object
                    if len(problems) > 0 :
                        # register problems found at previous data object
                        data_objects.append( (data_id, ref[3], ref[2], list(problems.keys()) ) ) 
                        problems = {}

                    # register attributes of first replica, will serve as reference
                    data_id = row[0]
                    ref = row
                    continue
                
                # this must be a next replica of the same data object
                # check if attributes are equal to those of first replica
                if row[4] != ref[4] :
                    problems['data_type'] = True
                if row[5] != ref[5] :
                    problems['data_size'] = True
                if row[6] != ref[6] or row[7] != ref[7] :
                   problems['data_owner'] = True
                    
            if len(problems) > 0 :
                # register problems found at previous data object
                data_objects.append( (data_id, ref[3], ref[2], list(problems.keys()) ) ) 

    except(Exception) as error:
        print_stderr(error)
        return []

    return data_objects




if __name__ == "__main__":
    pass
