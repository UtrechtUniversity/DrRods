#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import sys
import json

CONFIGFILE = 'database.ini'
SOURCEFILE = '/etc/irods/server_config.json'

try:
    with open(SOURCEFILE, 'rt') as f:
        irods_data = json.load(f)
except:
    print("Unable to open source file '{}'".format(SOURCEFILE))
    sys.exit(1)

print("iRODS server database configuration read from file '{}'".format(SOURCEFILE))

data = irods_data['plugin_configuration']['database']['postgres']

try:
    with open(CONFIGFILE, 'wt') as f:
        f.write('[postgresql]\n')
        f.write("db_host={}\n".format(data['db_host']))
        f.write("db_port={}\n".format(data['db_port']))
        f.write("db_name={}\n".format(data['db_name']))
        f.write("db_username={}\n".format(data['db_username']))
        f.write("db_password={}\n".format(data['db_password']))
except:
    print("Unable to write configuration to file '{}'".format(CONFIGFILE))
    sys.exit(1)

print("DrRods configuration written to file '{}'".format(CONFIGFILE))



