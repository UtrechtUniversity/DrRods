# Index to DrRods tools
To learn the syntax of a program, invoke it with option _-h_.

## find\_hardlinks
*We have run this program on a system with 70 million replicas.
Execution time was 12 minutes, internal memory usage 14 GB.*

iRODS expects data object to be addressable via only a single logical
name (which is the combination COLL\_NAME/DATA\_NAME).
This logical name is registered with each replica of the data object.

On our production zones, we have encountered a few isolated cases 
where some of the replicas
of a single data object have a different logical name. 
We will refer to phenomenon as a hardlinked data object, since the data object
identifies itself by more than one name, hence has aliases.

Hence an "ils" command may show 2 data objects while in fact there is
just one. In practice, this may lead to unexpected behavior when the 
user performs
an operation on one of these data objects, e.g. executes an "imv" or "irm"
command. It will turn out that the other object is affected as well.
Therefore, hardlinked data objects can potentially cause data loss.

The program locates hardlinked data objects and reports them. It is up
to the iRODS System Administrator to then manually inspect the objects 
in detail and to decide a repair strategy.  Our experience is that
the action to be taken changes from case to case.

A conservative strategy is to use "icp" to copy one alias name to 
a new destination data object, and then trim the replicas listed 
under the same alias name in the source object.  Make sure to also copy
the metadata (imeta) and authorizations (ichmod). 

Note that in case of overlapping replica numbers, an attempt to unregister
a replica by replica number might fail. 


## find\_inconsistent\_paths
*We have run this program on a system with 70 million replicas.
Execution time was 25 minutes, internal memory usage 1500 MB.*

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
a replica, where the column 'expected\_data\_path' depicts an alternative
path that is derived from the logical path.

Whenever a data\_path does no longer mimic the logical path, there exists
a risk of replicas becoming soft-linked during subsequent data operations. 

A soft-linked replica references a data file that is *also* referenced
by an entirely different data object. 
Note that this situation could lead to data loss: 
When a user deletes one of the 
involved data objects, then the linked replica of the other data object can no
longer access its data file, evn though it may appear to be 'good'.. 

Nowadays iRODS provides for logical locking of data objects and their
replicas. This should greatly assist to prevent any new occurrences of 
inconsistent paths.  

###
On a system with 70M replicas, 
  
 

