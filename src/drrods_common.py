#!/bin/python3
# 2024 by Ton Smeele, Utrecht University

# Warning: The pickle module is not secure (remote code injection risk).
#          Do not load any data from files created by other tools!

import sys
import socket
import contextlib
import pickle

# dict file signatures:
SIGNATURE_REPLICA_DATA = 'replica_data_paths'
SIGNATURE_ORPHAN_FILES = 'orphan_files'
SIGNATURE_ORPHAN_REPLICAS = 'orphan_replicas'


def localhost_name():
    return socket.getfqdn()


@contextlib.contextmanager
def open_w(filepath):
    """
    Returns (for write actions) a file handle to a textfile.
    Returns stdout if the argument is a dash.
    """
    if filepath and filepath != '-':
        f = open(filepath, 'w')
    else:
        f = sys.stdout

    try:
        yield f
    finally:
        if f is not sys.stdout:
            f.close()


def load_dict(filepath, signature='default'):
    db = {}
    try:
        with open(filepath, 'rb') as f:
            db = pickle.load(f)
    except (Exception) as error:
        print_stderr(error)
        return None

    if 'signature' in db and db['signature'] == signature:
        return db['dict_object']

    print_stderr('Error: File content does not meet expected signature ('
                 + filepath + ')')
    return None


def dump_dict(filepath, dict_object, signature='default'):
    db = {}
    db['signature'] = signature
    db['dict_object'] = dict_object
    # an except should probably be fatal?
    # we leave it up to the caller to handle any exception
    with open(filepath, 'wb') as f:
        pickle.dump(db, f)


def print_stderr(text):
    sys.stderr.write(str(text) + '\n')


def bash_squote(s):
    """
    Returns a single quote delimited string.
    Any single quote inside the string is transformed to a
    backslash-escaped quote that itself is delimited by single quotes.
    The resulting string can be used as an argument to commands
    in a Linux bash script
    """
    return "'" + s.replace("'", "'\\''") + "'"


def csv_dquote(s):
    """
    Returns a double quote delimited string.
    Any double quote inside the string is transformed to two double quotes, in
    line with RFC4180. The resulting string can be used as a field
    in a CSV file.
    """
    return '"' + s.replace('"', '""') + '"'
