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

In all commands, you will need to replace "instance", "zookeepers", "username", and "password" with the values you set for your Accumulo instance.

Commands intended to be run in bash are prefixed by '$'.  These are always assumed to be run from the $ACCUMULO_HOME directory.

Commands intended to be run in the Accumulo shell are prefixed by '>'.

[1]: {{ site.baseurl }}/user_manual_1.3-incubating/Accumulo_Shell.html#User_Administration
[aggregation](aggregation.html)

[batch](batch.html)

[bloom](bloom.html)

[bulkIngest](bulkIngest.html)

[constraints](constraints.html)

[dirlist](dirlist.html)

[filter](filter.html)

[helloworld](helloworld.html)

[mapred](mapred.html)

[shard](shard.html)

