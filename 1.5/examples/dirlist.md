---
title: File System Archive
---

This example stores filesystem information in accumulo.  The example stores the information in the following three tables.  More information about the table structures can be found at the end of README.dirlist.

 * directory table : This table stores information about the filesystem directory structure.
 * index table     : This table stores a file name index.  It can be used to quickly find files with given name, suffix, or prefix.
 * data table      : This table stores the file data.  File with duplicate data are only stored once.  

This example shows how to use Accumulo to store a file system history.  It has the following classes:

 * Ingest.java - Recursively lists the files and directories under a given path, ingests their names and file info into one Accumulo table, indexes the file names in a separate table, and the file data into a third table.
 * QueryUtil.java - Provides utility methods for getting the info for a file, listing the contents of a directory, and performing single wild card searches on file or directory names.
 * Viewer.java - Provides a GUI for browsing the file system information stored in Accumulo.
 * FileCount.java - Computes recursive counts over file system information and stores them back into the same Accumulo table.
 
To begin, ingest some data with Ingest.java.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.Ingest -i instance -z zookeepers -u username -p password --vis exampleVis --chunkSize 100000 /local/username/workspace

This may take some time if there are large files in the /local/username/workspace directory.  If you use 0 instead of 100000 on the command line, the ingest will run much faster, but it will not put any file data into Accumulo (the dataTable will be empty).
Note that running this example will create tables dirTable, indexTable, and dataTable in Accumulo that you should delete when you have completed the example.
If you modify a file or add new files in the directory ingested (e.g. /local/username/workspace), you can run Ingest again to add new information into the Accumulo tables.

To browse the data ingested, use Viewer.java.  Be sure to give the "username" user the authorizations to see the data (in this case, run

    $ ./bin/accumulo shell -u root -e 'setauths -u username -s exampleVis'

then run the Viewer:

    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.Viewer -i instance -z zookeepers -u username -p password -t dirTable --dataTable dataTable --auths exampleVis --path /local/username/workspace

To list the contents of specific directories, use QueryUtil.java.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.QueryUtil -i instance -z zookeepers -u username -p password -t dirTable --auths exampleVis --path /local/username
    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.QueryUtil -i instance -z zookeepers -u username -p password -t dirTable --auths exampleVis --path /local/username/workspace

To perform searches on file or directory names, also use QueryUtil.java.  Search terms must contain no more than one wild card and cannot contain "/".
*Note* these queries run on the _indexTable_ table instead of the dirTable table.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.QueryUtil -i instance -z zookeepers -u username -p password -t indexTable --auths exampleVis --path filename --search
    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.QueryUtil -i instance -z zookeepers -u username -p password -t indexTable --auths exampleVis --path 'filename*' --search
    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.QueryUtil -i instance -z zookeepers -u username -p password -t indexTable --auths exampleVis --path '*jar' --search
    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.QueryUtil -i instance -z zookeepers -u username -p password -t indexTable --auths exampleVis --path 'filename*jar' --search

To count the number of direct children (directories and files) and descendants (children and children's descendants, directories and files), run the FileCount over the dirTable table.
The results are written back to the same table.  FileCount reads from and writes to Accumulo.  This requires scan authorizations for the read and a visibility for the data written.
In this example, the authorizations and visibility are set to the same value, exampleVis.  See README.visibility for more information on visibility and authorizations.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.dirlist.FileCount -i instance -z zookeepers -u username -p password -t dirTable --auths exampleVis

## Directory Table

Here is a illustration of what data looks like in the directory table:

    row colf:colq [vis]    value
    000 dir:exec [exampleVis]    true
    000 dir:hidden [exampleVis]    false
    000 dir:lastmod [exampleVis]    1291996886000
    000 dir:length [exampleVis]    1666
    001/local dir:exec [exampleVis]    true
    001/local dir:hidden [exampleVis]    false
    001/local dir:lastmod [exampleVis]    1304945270000
    001/local dir:length [exampleVis]    272
    002/local/Accumulo.README \x7F\xFF\xFE\xCFH\xA1\x82\x97:exec [exampleVis]    false
    002/local/Accumulo.README \x7F\xFF\xFE\xCFH\xA1\x82\x97:hidden [exampleVis]    false
    002/local/Accumulo.README \x7F\xFF\xFE\xCFH\xA1\x82\x97:lastmod [exampleVis]    1308746481000
    002/local/Accumulo.README \x7F\xFF\xFE\xCFH\xA1\x82\x97:length [exampleVis]    9192
    002/local/Accumulo.README \x7F\xFF\xFE\xCFH\xA1\x82\x97:md5 [exampleVis]    274af6419a3c4c4a259260ac7017cbf1

The rows are of the form depth + path, where depth is the number of slashes ("/") in the path padded to 3 digits.  This is so that all the children of a directory appear as consecutive keys in Accumulo; without the depth, you would for example see all the subdirectories of /local before you saw /usr.
For directories the column family is "dir".  For files the column family is Long.MAX_VALUE - lastModified in bytes rather than string format so that newer versions sort earlier.

## Index Table

Here is an illustration of what data looks like in the index table:

    row colf:colq [vis]
    fAccumulo.README i:002/local/Accumulo.README [exampleVis]
    flocal i:001/local [exampleVis]
    rEMDAER.olumuccA i:002/local/Accumulo.README [exampleVis]
    rlacol i:001/local [exampleVis]

The values of the index table are null.  The rows are of the form "f" + filename or "r" + reverse file name.  This is to enable searches with wildcards at the beginning, middle, or end.

## Data Table

Here is an illustration of what data looks like in the data table:

    row colf:colq [vis]    value
    274af6419a3c4c4a259260ac7017cbf1 refs:e77276a2b56e5c15b540eaae32b12c69\x00filext [exampleVis]    README
    274af6419a3c4c4a259260ac7017cbf1 refs:e77276a2b56e5c15b540eaae32b12c69\x00name [exampleVis]    /local/Accumulo.README
    274af6419a3c4c4a259260ac7017cbf1 ~chunk:\x00\x0FB@\x00\x00\x00\x00 [exampleVis]    *******************************************************************************\x0A1. Building\x0A\x0AIn the normal tarball or RPM release of accumulo, [truncated]
    274af6419a3c4c4a259260ac7017cbf1 ~chunk:\x00\x0FB@\x00\x00\x00\x01 [exampleVis]

The rows are the md5 hash of the file.  Some column family : column qualifier pairs are "refs" : hash of file name + null byte + property name, in which case the value is property value.  There can be multiple references to the same file which are distinguished by the hash of the file name.
Other column family : column qualifier pairs are "~chunk" : chunk size in bytes + chunk number in bytes, in which case the value is the bytes for that chunk of the file.  There is an end of file data marker whose chunk number is the number of chunks for the file and whose value is empty.

There may exist multiple copies of the same file (with the same md5 hash) with different chunk sizes or different visibilities.  There is an iterator that can be set on the data table that combines these copies into a single copy with a visibility taken from the visibilities of the file references, e.g. (vis from ref1)|(vis from ref2). 
