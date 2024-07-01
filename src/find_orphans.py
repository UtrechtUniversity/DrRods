#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import sys
import os
from stat import *
import icat
import pathdb
from drrods_common import *
import drrods_sql
import argparse



def find_inaccessible_files(db):
    inaccessible = pathdb.Pathdb()
    for filepath in db.iter():
        try:
            statinfo = os.stat(filepath)
            if not S_ISREG(statinfo.st_mode):
                # exist, but not a regular file
                inaccessible.register(filepath)
        except:
            # does not exist
            inaccessible.register(filepath)
    return inaccessible


def find_orphan_data_files(replica_paths, local_vault_dir, orphans):
    for dirpath, subdirs, files in os.walk(local_vault_dir):
        for filename in files:
            filepath = dirpath + os.sep + filename
            if not replica_paths.is_registered(filepath):
                orphans.register(filepath)
    return orphans


def retrieve_icat_data_from_database(connection, host):
    db = {}
    db['host'] = host
    db['vaults'] = drrods_sql.resource_vault_dirs_for_host(connection, host)
    (db['replica_count'], db['replica_data_paths']) = drrods_sql.replica_paths_for_host(connection, host) 
    if not db['vaults'] or not db['replica_data_paths']:
        db = None
    return db



def main(cmd):
    if cmd._import and cmd._host:
        print('Warning: Incompatible argument (hostname): Hostname will be derived from import file')

    # (1) obtain ICAT data either from the database or from an import data file
    db = {}
    localhost = localhost_name()
    if cmd._import:
        # import ICAT data from file
        filepath = cmd._import[0]
        print('Importing ICAT data from file {}'.format(filepath))
        db = load_dict(filepath, SIGNATURE_REPLICA_DATA)
    else:
        # retrieve ICAT data from database
        connection = icat.Icat()
        print(connection)
        if not connection.is_connected():
            print('Error: need a database connection to proceed')
            sys.exit(1)

        target_host = localhost
        if cmd._host:
            target_host = cmd._host[0]

        print('Retrieving data on host {} from ICAT database'.format(target_host))
        db = retrieve_icat_data_from_database(connection, target_host)

    if not db:
        sys.exit(1)

    # (2) inform the user of data that has been loaded or retrieved
    if db['host']  == localhost_name():
        print('Using ICAT data on local host ({}).'.format(db['host']))
    else:
        print('Using ICAT data that pertains to another host ({}).'.format(db['host']))

    vault_count = len(db['vaults'])
    replica_count = db['replica_count']
    data_path_count = db['replica_data_paths'].size()

    print('''
We have loaded data on: 
{} resource vaults
{} replicas
{} unique data paths
'''.format(vault_count, replica_count, data_path_count))

    # (3) if requested, export the ICAT data to a file 
    if cmd._export:
        filepath = cmd._export[0]
        print('Exporting ICAT data to file {}'.format(filepath))
        dump_dict(filepath, db, SIGNATURE_REPLICA_DATA)
       
        # in case of an export, we're done
        sys.exit(0)


    # (4) compare ICAT data with actual data files to locate any discrepancies (orphans)
    if db['host'] != localhost:
        print('Skipping scan for orphans/missing files because the data pertains to another host')
        sys.exit(0)

    # (4a) find data files in vaults that are not registered in the ICAT (orphan data files)
    print('Scanning iRODS resource vault dirs for orphan files...(this may take awhile)')
    count = 0
    orphan_files = pathdb.Pathdb()  
    for vault_dir in db['vaults']:
        # print('Orphan file scan in vault directory {}'.format(vault_dir))
        find_orphan_data_files(db['replica_data_paths'], vault_dir, orphan_files )
        orphans_in_vault = orphan_files.size() - count
        count = count + orphans_in_vault
        print('{} orphan files in vault {}'.format(orphans_in_vault, vault_dir))
    
    print('\n{} orphans in total in above vaults'.format(count))

    # (4b) find replicas that reference a non-existing data file  (orphan replicas)
    print('\nChecking for data files referenced by replicas, yet missing in vault:')
    orphan_replicas = find_inaccessible_files(db['replica_data_paths'])
    print('{} inaccessible data files found\n'.format(orphan_replicas.size()))

    # (5) store output data as dict file (suitable for processing by compagnion tools)
    if cmd.summary:
        # user only wants a summary, no output file needed
        sys.exit(0)

    path_orphan_files = "orphan_files_" + db['host'] + ".bin"
    path_orphan_replicas = "orphan_replicas_" + db['host'] + ".bin"
    orphans = {}
    orphans['host'] = db['host']
    
    if orphan_files.size() > 0 :
        orphans['file_paths'] = orphan_files
        dump_dict("orphan_files_" + db['host'] + ".bin", orphans, SIGNATURE_ORPHAN_FILES)
        print("Data on orphan files written to file:    " + path_orphan_files)


    if orphan_replicas.size() > 0:
        orphans['file_paths'] = None
        orphans['replica_data_paths'] = orphan_replicas
        dump_dict("orphan_replicas_" + db['host'] + ".bin", orphans, SIGNATURE_ORPHAN_REPLICAS)
        print("Data on orphan replicas written to file: " + path_orphan_replicas)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
            description='''
Finds data files not referenced by replicas and vice versa
''', epilog = '''
The analysis output is written to 2 output files: 
orphan_files_<hostname>.bin    - files in the vault yet not registered as a replica
orphan_replicas_<hostname>.bin - replicas that reference a non-existing data file

The output files are in a binary format, suitable for processing by other DrRods tools
'''
)
    parser.add_argument('-i', '--import', nargs=1, metavar='<filename>', dest='_import',
            help='import ICAT data from a file (instead of retrieving from the database)')
    parser.add_argument('-e','--export', nargs=1, metavar='<filename>', dest='_export',
            help='export retrieved/imported ICAT data to a file')
    parser.add_argument('-H','--host', nargs=1, metavar='<hostname>', dest='_host',
            help='use data pertaining to this host (fully-qualified domain name)') 
    parser.add_argument('-s','-summary', action='store_true', dest='summary',
            help='only provide summary of findings, do not generate output data files')
    cmd = parser.parse_args()
    main(cmd)
