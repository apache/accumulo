---
title: Constraints Example
---

This tutorial uses the following Java classes, which can be found in org.apache.accumulo.examples.simple.constraints in the examples-simple module:

 * AlphaNumKeyConstraint.java - a constraint that requires alphanumeric keys
 * NumericValueConstraint.java - a constraint that requires numeric string values

This an example of how to create a table with constraints. Below a table is
created with two example constraints.  One constraints does not allow non alpha
numeric keys.  The other constraint does not allow non numeric values. Two
inserts that violate these constraints are attempted and denied.  The scan at
the end shows the inserts were not allowed. 

    $ ./bin/accumulo shell -u username -p password
    
    Shell - Apache Accumulo Interactive Shell
    - 
    - version: 1.5.0
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> createtable testConstraints
    username@instance testConstraints> constraint -a org.apache.accumulo.examples.simple.constraints.NumericValueConstraint
    username@instance testConstraints> constraint -a org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint
    username@instance testConstraints> insert r1 cf1 cq1 1111
    username@instance testConstraints> insert r1 cf1 cq1 ABC
      Constraint Failures:
          ConstraintViolationSummary(constrainClass:org.apache.accumulo.examples.simple.constraints.NumericValueConstraint, violationCode:1, violationDescription:Value is not numeric, numberOfViolatingMutations:1)
    username@instance testConstraints> insert r1! cf1 cq1 ABC 
      Constraint Failures:
          ConstraintViolationSummary(constrainClass:org.apache.accumulo.examples.simple.constraints.NumericValueConstraint, violationCode:1, violationDescription:Value is not numeric, numberOfViolatingMutations:1)
          ConstraintViolationSummary(constrainClass:org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint, violationCode:1, violationDescription:Row was not alpha numeric, numberOfViolatingMutations:1)
    username@instance testConstraints> scan
    r1 cf1:cq1 []    1111
    username@instance testConstraints> 

