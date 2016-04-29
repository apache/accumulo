---
title: Glossary
nav: nav_glossary
---


authorizations
: a set of strings associated with a user or with a particular scan that will
be used to determine which key/value pairs are visible to the user.

cell
: a set of key/value pairs whose keys differ only in timestamp.

column
: the portion of the key that sorts after the row and is divided into family,
qualifier, and visibility.

column family
: the portion of the key that sorts second and controls locality groups, the
row/column hybrid nature of accumulo.

column qualifier
: the portion of the key that sorts third and provides additional key
uniqueness.

column visibility
: the portion of the key that sorts fourth and controls user access to
individual key/value pairs. Visibilities are boolean AND (&amp;) and OR (|)
combinations of authorization strings with parentheses required to determine
ordering, e.g. (AB&amp;C)|DEF.

iterator
: a mechanism for modifying tablet-local portions of the key/value space.
Iterators are used for standard administrative tasks as well as for custom
processing.

iterator priority
: an iterator must be configured with a particular scope and priority. When a
tablet server enters that scope, it will instantiate iterators in priority
order starting from the smallest priority and ending with the largest, and
apply each to the data read before rewriting the data or sending the data to
the user.

iterator scopes
: the possible scopes for iterators are where the tablet server is already
reading and/or writing data: minor compaction / flush time (*minc*
scope), major compaction / file merging time (*majc* scope), and query
time (*scan* scope).

gc
: process that identifies temporary files in HDFS that are no longer needed by
any process, and deletes them.

key
: the key into the distributed sorted map which is accumulo. The key is
subdivided into row, column, and timestamp. The column is further divided into
family, qualifier, and visibility.

locality group
: a set of column families that will be grouped together on disk. With no
locality groups configured, data is stored on disk in row order. If each
column family were configured to be its own locality group, the data for each
column would be stored separately, in row order. Configuring sets of columns
into locality groups is a compromise between the two approaches and will
improve performance when multiple columns are accessed in the same scan.

log-structured merge-tree
: the sorting / flushing / merging scheme on which BigTable's design is based.

logger
: in 1.4 and older, process that accepts updates to tablet servers and writes
them to local on-disk storage for redundancy. in 1.5 the functionality was
subsumed by the tablet server and datanode with HDFS writes.

major compaction
: merging multiple files into a single file. If all of a tablet's files are
merged into a single file, it is called a *full major compaction*.

master
: process that detects and responds to tablet failures, balances load across
tablet servers by assigning and migrating tablets when required, coordinates
table operations, and handles tablet server logistics (startup, shutdown,
recovery).

minor compaction
: flushing data from memory to disk. Usually this creates a new file for a
tablet, but if the memory flushed is merge-sorted in with data from an existing
file (replacing that file), it is called a *merging minor compaction*.

monitor
: process that displays status and usage information for all Accumulo
components.

permissions
: administrative abilities that must be given to a user such as creating tables
or users and changing permissions or configuration parameters.

row
: the portion of the key that controls atomicity. Keys with the same row are
guaranteed to remain on a single tablet hosted by a single tablet server,
therefore multiple key/value pairs can be added to or removed from a row at the
same time. The row is used for the primary sorting of the key.

scan
: reading a range of key/value pairs.

tablet
: a contiguous key range; the unit of work for a tablet server.

tablet servers
: a set of servers that hosts reads and writes for tablets. Each server hosts
a distinct set of tablets at any given time, but the tablets may be hosted by
different servers over time.

timestamp
: the portion of the key that controls versioning. Otherwise identical keys
with differing timestamps are considered to be versions of a single
*cell*. Accumulo can be configured to keep the *N* newest
versions of each *cell*. When a deletion entry is inserted, it deletes
all earlier versions for its cell.

value
: immutable bytes associated with a particular key.

