---
title: Constraints Example
---

This an example of how to create a table with constraints. Below a table is
create with two example constraints.  One constraints does not allow non alpha
numeric keys.  The other constraint does not allow non numeric values. Two
inserts that violate these constraints are attempted and denied.  The scan at
the end shows the inserts were not allowed. 

    $ ./bin/accumulo shell -u username -p pass
    
    Shell - Apache Accumulo Interactive Shell
    - 
    - version: 1.3.x-incubating
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> createtable testConstraints
    username@instance testConstraints> config -t testConstraints -s table.constraint.1=org.apache.accumulo.examples.constraints.NumericValueConstraint
    username@instance testConstraints> config -t testConstraints -s table.constraint.2=org.apache.accumulo.examples.constraints.AlphaNumKeyConstrain                                                                                                    
    username@instance testConstraints> insert r1 cf1 cq1 1111
    username@instance testConstraints> insert r1 cf1 cq1 ABC
      Constraint Failures:
          ConstraintViolationSummary(constrainClass:org.apache.accumulo.examples.constraints.NumericValueConstraint, violationCode:1, violationDescription:Value is not numeric, numberOfViolatingMutations:1)
    username@instance testConstraints> insert r1! cf1 cq1 ABC 
      Constraint Failures:
          ConstraintViolationSummary(constrainClass:org.apache.accumulo.examples.constraints.NumericValueConstraint, violationCode:1, violationDescription:Value is not numeric, numberOfViolatingMutations:1)
          ConstraintViolationSummary(constrainClass:org.apache.accumulo.examples.constraints.AlphaNumKeyConstraint, violationCode:1, violationDescription:Row was not alpha numeric, numberOfViolatingMutations:1)
    username@instance testConstraints> scan
    r1 cf1:cq1 []    1111
    username@instance testConstraints> 
