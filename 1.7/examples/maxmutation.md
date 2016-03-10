---
title: MaxMutation Constraints Example
---

This an example of how to limit the size of mutations that will be accepted into
a table. Under the default configuration, accumulo does not provide a limitation
on the size of mutations that can be ingested. Poorly behaved writers might
inadvertently create mutations so large, that they cause the tablet servers to
run out of memory. A simple contraint can be added to a table to reject very
large mutations.

    $ ./bin/accumulo shell -u username -p password

    Shell - Apache Accumulo Interactive Shell
    -
    - version: 1.6.0
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    -
    - type 'help' for a list of available commands
    -
    username@instance> createtable test_ingest
    username@instance test_ingest> config -t test_ingest -s table.constraint.1=org.apache.accumulo.examples.simple.constraints.MaxMutationSize
    username@instance test_ingest>


Now the table will reject any mutation that is larger than 1/256th of the
working memory of the tablet server. The following command attempts to ingest
a single row with 10000 columns, which exceeds the memory limit:

    $ ./bin/accumulo org.apache.accumulo.test.TestIngest -i instance -z zookeepers -u username -p password --rows 1 --cols 10000
    ERROR : Constraint violates : ConstraintViolationSummary(constrainClass:org.apache.accumulo.examples.simple.constraints.MaxMutationSize, violationCode:0, violationDescription:mutation exceeded maximum size of 188160, numberOfViolatingMutations:1)

