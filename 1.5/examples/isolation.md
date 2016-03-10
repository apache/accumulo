---
title: Isolation Example
---

Accumulo has an isolated scanner that ensures partial changes to rows are not
seen.  Isolation is documented in ../docs/isolation.html and the user manual.  

InterferenceTest is a simple example that shows the effects of scanning with
and without isolation.  This program starts two threads.  One threads
continually upates all of the values in a row to be the same thing, but
different from what it used to be.  The other thread continually scans the
table and checks that all values in a row are the same.  Without isolation the
scanning thread will sometimes see different values, which is the result of
reading the row at the same time a mutation is changing the row.

Below, Interference Test is run without isolation enabled for 5000 iterations
and it reports problems.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.isolation.InterferenceTest -i instance -z zookeepers -u username -p password -t isotest --iterations 5000
    ERROR Columns in row 053 had multiple values [53, 4553]
    ERROR Columns in row 061 had multiple values [561, 61]
    ERROR Columns in row 070 had multiple values [570, 1070]
    ERROR Columns in row 079 had multiple values [1079, 1579]
    ERROR Columns in row 088 had multiple values [2588, 1588]
    ERROR Columns in row 106 had multiple values [2606, 3106]
    ERROR Columns in row 115 had multiple values [4615, 3115]
    finished

Below, Interference Test is run with isolation enabled for 5000 iterations and
it reports no problems.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.isolation.InterferenceTest -i instance -z zookeepers -u username -p password -t isotest --iterations 5000 --isolated
    finished


