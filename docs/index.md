# Index to DrRods tools

## find\_inconsistent\_paths
Unless the storage system is less suited to accomodate this, iRODS will
attempt to create and maintain a directory structure for data files that 
mimics the collection hierarchy. 
In particular it uses this strategy for unixfilesystem resources.

Incomplete or incorrectly executed iRODS operations may result in
persistent data that no longer mimics the collection hierarchy.

Potential causes are prematurely crashed iRODS Agents, bugs in the iRODS 
Data Virtualization software. Another cause can be a policy added
to the system, that has side-effects that impact the consistency
of iRODS operations.

This program locates replicas which have an attribute data\_path
that does not properly mimic the logical path. 
Replicas that reference a data file located outside the vault directory 
are ignored.
The output is a csv-formatted file. Each row holds information on 
a replica, where the column 'expected\_data\_path' depict an alternative
path that is derived from the logical path.

Whenever a data\_path does no longer mimic the logical path, there exist
a risk of replicas becoming soft-linked during subsequent data operations. 

Soft-linked replicas means that replicas of different data objects 
reference the same data file. 
Note that this could lead to data loss: When a user deletes one of the 
involved data objects, then the replica of the other data object can no
longer access its data file. 

Nowadays iRODS provides for logical locking of data objects and their
replicas. This should greatly assist to prevent any new occurrences of 
inconsistent paths.  


  
 

