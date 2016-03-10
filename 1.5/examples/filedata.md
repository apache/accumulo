---
title: File System Archive Example (Data Only)
---

This example archives file data into an Accumulo table.  Files with duplicate data are only stored once.
The example has the following classes:

 * CharacterHistogram - A MapReduce that computes a histogram of byte frequency for each file and stores the histogram alongside the file data.  An example use of the ChunkInputFormat.
 * ChunkCombiner - An Iterator that dedupes file data and sets their visibilities to a combined visibility based on current references to the file data.
 * ChunkInputFormat - An Accumulo InputFormat that provides keys containing file info (List<Entry<Key,Value>>) and values with an InputStream over the file (ChunkInputStream).
 * ChunkInputStream - An input stream over file data stored in Accumulo.
 * FileDataIngest - Takes a list of files and archives them into Accumulo keyed on hashes of the files.
 * FileDataQuery - Retrieves file data based on the hash of the file. (Used by the dirlist.Viewer.)
 * KeyUtil - A utility for creating and parsing null-byte separated strings into/from Text objects.
 * VisibilityCombiner - A utility for merging visibilities into the form (VIS1)|(VIS2)|...

This example is coupled with the dirlist example.  See README.dirlist for instructions.

If you haven't already run the README.dirlist example, ingest a file with FileDataIngest.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.filedata.FileDataIngest -i instance -z zookeepers -u username -p password -t dataTable --auths exampleVis --chunk 1000 $ACCUMULO_HOME/README

Open the accumulo shell and look at the data.  The row is the MD5 hash of the file, which you can verify by running a command such as 'md5sum' on the file.

    > scan -t dataTable

Run the CharacterHistogram MapReduce to add some information about the file.

    $ bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.filedata.CharacterHistogram -i instance -z zookeepers -u username -p password -t dataTable --auths exampleVis --vis exampleVis

Scan again to see the histogram stored in the 'info' column family.

    > scan -t dataTable
