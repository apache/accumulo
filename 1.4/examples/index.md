---
title: Examples
---

Each README in the examples directory highlights the use of particular features of Apache Accumulo.

Before running any of the examples, the following steps must be performed.

1. Install and run Accumulo via the instructions found in $ACCUMULO_HOME/README.
Remember the instance name.  It will be referred to as "instance" throughout the examples.
A comma-separated list of zookeeper servers will be referred to as "zookeepers".

2. Create an Accumulo user (see the [user manual][1]), or use the root user.
The Accumulo user name will be referred to as "username" with password "password" throughout the examples.
This user will need to have the ability to create tables.

In all commands, you will need to replace "instance", "zookeepers", "username", and "password" with the values you set for your Accumulo instance.

Commands intended to be run in bash are prefixed by '$'.  These are always assumed to be run from the $ACCUMULO_HOME directory.

Commands intended to be run in the Accumulo shell are prefixed by '>'.

[1]: {{ site.baseurl }}/1.4/user_manual/Accumulo_Shell#User_Administration
[batch](batch)

[bloom](bloom)

[bulkIngest](bulkIngest)

[combiner](combiner)

[constraints](constraints)

[dirlist](dirlist)

[filedata](filedata)

[filter](filter)

[helloworld](helloworld)

[isolation](isolation)

[mapred](mapred)

[shard](shard)

[visibility](visibility)

