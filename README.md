# DrRods
DrRods is a small collection of Python programs that can be used to analyze the health 
of the persistent data virtualization data of an [iRODS](https://irods.org) system.

It assists the iRODS system administrator in localizing any issues with persistent virtualization data.

Types of issues it can locate:
- Hardlinked data object - a data object that has more than one logical name
- Softlinked replicas - replicas that reference the same data file
- Orphaned data files - data files in iRODS vaults that are not registered in the ICAT
- Orphaned replicas - replicas that reference a data file that is inaccessible/does not exist

The DrRods tools have been designed with performance in mind. 
The toolset only uses SQL queries that can execute 
relatively fast on a large iRODS ICAT database, and it keeps track of file paths via an efficient
in-memory data structure.

The toolset is described in more detail [here](./docs/index.md)

## Installation
The tools can be installed on an iRODS provider server.

Create a virtual environment:
```
python -m venv /path/to/new/virtual/environment
```

Activate the virtual environment
```
source /path/to/new/virtual/environment/bin/activate
```

Install dependency: psycopg 
```
pip install --upgrade pip           # upgrade pip to at least 20.3
pip install "psycopg[binary]"       # remove [binary] for PyPy
```
This is a relatively new Python Postgresql driver library, a successor of psycopg2.
For information on this package see the [psycopg documentation](https://www.psycopg.org/psycopg3/docs/basic/install.html).
Should you need to use the psycopg2 package, then edit the DrRods module icat.py to change
this dependency.

Install DrRods tools
```
git clone https://github.com/UtrechtUniversity/DrRods.git
```


