---
title: Visibility, Authorizations, and Permissions Example
---

## Creating a new user

    root@instance> createuser username
    Enter new password for 'username': ********
    Please confirm new password for 'username': ********
    root@instance> user username
    Enter password for user username: ********
    username@instance> createtable vistest
    06 10:48:47,931 [shell.Shell] ERROR: org.apache.accumulo.core.client.AccumuloSecurityException: Error PERMISSION_DENIED - User does not have permission to perform this action
    username@instance> userpermissions
    System permissions: 
    
    Table permissions (!METADATA): Table.READ
    username@instance> 

A user does not by default have permission to create a table.

## Granting permissions to a user

    username@instance> user root
    Enter password for user root: ********
    root@instance> grant -s System.CREATE_TABLE -u username
    root@instance> user username 
    Enter password for user username: ********
    username@instance> createtable vistest
    username@instance> userpermissions
    System permissions: System.CREATE_TABLE
    
    Table permissions (!METADATA): Table.READ
    Table permissions (vistest): Table.READ, Table.WRITE, Table.BULK_IMPORT, Table.ALTER_TABLE, Table.GRANT, Table.DROP_TABLE
    username@instance vistest> 

## Inserting data with visibilities

Visibilities are boolean AND (&) and OR (|) combinations of authorization
tokens.  Authorization tokens are arbitrary strings taken from a restricted 
ASCII character set.  Parentheses are required to specify order of operations 
in visibilities.

    username@instance vistest> insert row f1 q1 v1 -l A
    username@instance vistest> insert row f2 q2 v2 -l A&B
    username@instance vistest> insert row f3 q3 v3 -l apple&carrot|broccoli|spinach
    06 11:19:01,432 [shell.Shell] ERROR: org.apache.accumulo.core.util.BadArgumentException: cannot mix | and & near index 12
    apple&carrot|broccoli|spinach
                ^
    username@instance vistest> insert row f3 q3 v3 -l (apple&carrot)|broccoli|spinach
    username@instance vistest> 

## Scanning with authorizations

Authorizations are sets of authorization tokens.  Each Accumulo user has 
authorizations and each Accumulo scan has authorizations.  Scan authorizations 
are only allowed to be a subset of the user's authorizations.  By default, a 
user's authorizations set is empty.

    username@instance vistest> scan
    username@instance vistest> scan -s A
    06 11:43:14,951 [shell.Shell] ERROR: java.lang.RuntimeException: org.apache.accumulo.core.client.AccumuloSecurityException: Error BAD_AUTHORIZATIONS - The user does not have the specified authorizations assigned
    username@instance vistest> 

## Setting authorizations for a user

    username@instance vistest> setauths -s A
    06 11:53:42,056 [shell.Shell] ERROR: org.apache.accumulo.core.client.AccumuloSecurityException: Error PERMISSION_DENIED - User does not have permission to perform this action
    username@instance vistest> 

A user cannot set authorizations unless the user has the System.ALTER_USER permission.
The root user has this permission.

    username@instance vistest> user root
    Enter password for user root: ********
    root@instance vistest> setauths -s A -u username
    root@instance vistest> user username
    Enter password for user username: ********
    username@instance vistest> scan -s A
    row f1:q1 [A]    v1
    username@instance vistest> scan
    row f1:q1 [A]    v1
    username@instance vistest> 

The default authorizations for a scan are the user's entire set of authorizations.

    username@instance vistest> user root
    Enter password for user root: ********
    root@instance vistest> setauths -s A,B,broccoli -u username
    root@instance vistest> user username
    Enter password for user username: ********
    username@instance vistest> scan
    row f1:q1 [A]    v1
    row f2:q2 [A&B]    v2
    row f3:q3 [(apple&carrot)|broccoli|spinach]    v3
    username@instance vistest> scan -s B
    username@instance vistest> 
    
If you want, you can limit a user to only be able to insert data which they can read themselves.
It can be set with the following constraint.

    username@instance vistest> user root
    Enter password for user root: ******
    root@instance vistest> config -t vistest -s table.constraint.1=org.apache.accumulo.core.security.VisibilityConstraint    
    root@instance vistest> user username
    Enter password for user username: ********
    username@instance vistest> insert row f4 q4 v4 -l spinach                                                                
        Constraint Failures:
            ConstraintViolationSummary(constrainClass:org.apache.accumulo.core.security.VisibilityConstraint, violationCode:2, violationDescription:User does not have authorization on column visibility, numberOfViolatingMutations:1)
    username@instance vistest> insert row f4 q4 v4 -l spinach|broccoli
    username@instance vistest> scan
    row f1:q1 [A]    v1
    row f2:q2 [A&B]    v2
    row f3:q3 [(apple&carrot)|broccoli|spinach]    v3
    row f4:q4 [spinach|broccoli]    v4
    username@instance vistest> 
    
