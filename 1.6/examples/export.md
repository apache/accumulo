---
title: Export/Import Example
---

Accumulo provides a mechanism to export and import tables. This README shows
how to use this feature.

The shell session below shows creating a table, inserting data, and exporting
the table. A table must be offline to export it, and it should remain offline
for the duration of the distcp. An easy way to take a table offline without
interuppting access to it is to clone it and take the clone offline.

    root@test16> createtable table1
    root@test16 table1> insert a cf1 cq1 v1
    root@test16 table1> insert h cf1 cq1 v2
    root@test16 table1> insert z cf1 cq1 v3
    root@test16 table1> insert z cf1 cq2 v4
    root@test16 table1> addsplits -t table1 b r
    root@test16 table1> scan
    a cf1:cq1 []    v1
    h cf1:cq1 []    v2
    z cf1:cq1 []    v3
    z cf1:cq2 []    v4
    root@test16> config -t table1 -s table.split.threshold=100M
    root@test16 table1> clonetable table1 table1_exp
    root@test16 table1> offline table1_exp
    root@test16 table1> exporttable -t table1_exp /tmp/table1_export
    root@test16 table1> quit

After executing the export command, a few files are created in the hdfs dir.
One of the files is a list of files to distcp as shown below.

    $ hadoop fs -ls /tmp/table1_export
    Found 2 items
    -rw-r--r--   3 user supergroup        162 2012-07-25 09:56 /tmp/table1_export/distcp.txt
    -rw-r--r--   3 user supergroup        821 2012-07-25 09:56 /tmp/table1_export/exportMetadata.zip
    $ hadoop fs -cat /tmp/table1_export/distcp.txt
    hdfs://n1.example.com:6093/accumulo/tables/3/default_tablet/F0000000.rf
    hdfs://n1.example.com:6093/tmp/table1_export/exportMetadata.zip

Before the table can be imported, it must be copied using distcp. After the
discp completed, the cloned table may be deleted.

    $ hadoop distcp -f /tmp/table1_export/distcp.txt /tmp/table1_export_dest

The Accumulo shell session below shows importing the table and inspecting it.
The data, splits, config, and logical time information for the table were
preserved.

    root@test16> importtable table1_copy /tmp/table1_export_dest
    root@test16> table table1_copy
    root@test16 table1_copy> scan
    a cf1:cq1 []    v1
    h cf1:cq1 []    v2
    z cf1:cq1 []    v3
    z cf1:cq2 []    v4
    root@test16 table1_copy> getsplits -t table1_copy
    b
    r
    root@test16> config -t table1_copy -f split
    ---------+--------------------------+-------------------------------------------
    SCOPE    | NAME                     | VALUE
    ---------+--------------------------+-------------------------------------------
    default  | table.split.threshold .. | 1G
    table    |    @override ........... | 100M
    ---------+--------------------------+-------------------------------------------
    root@test16> tables -l
    accumulo.metadata    =>        !0
    accumulo.root        =>        +r
    table1_copy          =>         5
    trace                =>         1
    root@test16 table1_copy> scan -t accumulo.metadata -b 5 -c srv:time
    5;b srv:time []    M1343224500467
    5;r srv:time []    M1343224500467
    5< srv:time []    M1343224500467


