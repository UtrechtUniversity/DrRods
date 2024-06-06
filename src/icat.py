#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

import configparser
import psycopg
import sys

CONFIG_PATH='./database.ini'

class Icat(object):
    """
    Icat handles the connection with a PostgresSQL based iRODS ICAT database

    Use this class as follows: 

    db = Icat()
    if not db.is_connected():
       # could not connect, handle error
    try:
       with db.cursor() as cur:
          cur.execute("select foo from bar")
          for row in db.iter_row(cur):
             print(row[0])       (do something fancy with the row)
    except (Exception) as error:
       print(error)

    Icat uses details from a configuration file '{}' to connect
    to a PostgreSQL database. 
    The configuration file must be formatted as a textfile 
    consisting of sections that have key/value pairs according to
    the following example:

    [postgresql]
    db_username=irodsdb
    db_password=secret
    db_host=localhost
    db_port=5432
    db_name=icat

    """.format(CONFIG_PATH)

    def __init__(self, path = CONFIG_PATH):
        self._connection = None
        self._name = "Not connected to a database"
        try:
            config = self._load_config(path, 'postgresql')
            target = "'{}' at {}".format(config['db_name'], config['db_host'])
            self._connection = psycopg.connect(
                user     = config['db_username'],
                password = config['db_password'],
                host     = config['db_host'],
                port     = config['db_port'],
                dbname   = config['db_name'])
        except (psycopg.DatabaseError, Exception) as error:
            sys.stderr.write("Error while connecting to PostgreSQL database {}:\n{}\n".format(target, error))
            return

        self._name = "Connected to database {}".format(target)

    def __del__(self):
        try:
            self._connection.close()
        except:
            pass

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self._name 

    def is_connected(self):
        if (self._connection is None):
            return False
        return True

    def iter_row(self, cursor, size=256):
        while True:
            rows = cursor.fetchmany(size)
            if not rows:
                break
            for row in rows:
                yield row

    def cursor(self):
        return self._connection.cursor()


    def _load_config(self, filepath, section):
        parser = configparser.ConfigParser()
        parser.read(filepath)
        config = {}
        if parser.has_section(section):
            for item in parser.items(section):
                config[item[0]] = item[1]
        return config


if __name__ == "__main__":
    # execute connectivity test
    connection = Icat()
    print(connection)

