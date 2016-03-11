---
title: File System Archive
---

This example shows how to use Accumulo to store a file system history.  It has three classes:

 * Ingest.java - Recursively lists the files and directories under a given path, ingests their names and file info (not the file data!) into an Accumulo table, and indexes the file names in a separate table.
 * QueryUtil.java - Provides utility methods for getting the info for a file, listing the contents of a directory, and performing single wild card searches on file or directory names.
 * Viewer.java - Provides a GUI for browsing the file system information stored in Accumulo.
 * FileCountMR.java - Runs MR over the file system information and writes out counts to an Accumulo table.
 * FileCount.java - Accomplishes the same thing as FileCountMR, but in a different way.  Computes recursive counts and stores them back into table.
 * StringArraySummation.java - Aggregates counts for the FileCountMR reducer.
 
To begin, ingest some data with Ingest.java.

    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.Ingest instance zookeepers username password direxample dirindex exampleVis /local/user1/workspace

Note that running this example will create tables direxample and dirindex in Accumulo that you should delete when you have completed the example.
If you modify a file or add new files in the directory ingested (e.g. /local/user1/workspace), you can run Ingest again to add new information into the Accumulo tables.

To browse the data ingested, use Viewer.java.  Be sure to give the "username" user the authorizations to see the data.

    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.Viewer instance zookeepers username password direxample exampleVis /local/user1/workspace

To list the contents of specific directories, use QueryUtil.java.

    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.QueryUtil instance zookeepers username password direxample exampleVis /local/user1
    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.QueryUtil instance zookeepers username password direxample exampleVis /local/user1/workspace

To perform searches on file or directory names, also use QueryUtil.java.  Search terms must contain no more than one wild card and cannot contain "/".
*Note* these queries run on the _dirindex_ table instead of the direxample table.

    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.QueryUtil instance zookeepers username password dirindex exampleVis filename -search
    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.QueryUtil instance zookeepers username password dirindex exampleVis 'filename*' -search
    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.QueryUtil instance zookeepers username password dirindex exampleVis '*jar' -search
    $ ./bin/accumulo org.apache.accumulo.examples.dirlist.QueryUtil instance zookeepers username password dirindex exampleVis filename*jar -search

To count the number of direct children (directories and files) and descendants (children and children's descendents, directories and files), run the FileCountMR over the direxample table.
The results can be written back to the same table.

    $ ./bin/tool.sh lib/accumulo-examples-*[^c].jar org.apache.accumulo.examples.dirlist.FileCountMR instance zookeepers username password direxample direxample exampleVis exampleVis

Alternatively, you can also run FileCount.java.
