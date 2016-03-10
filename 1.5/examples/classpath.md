---
title: Classpath Example
---

This example shows how to use per table classpaths.   The example leverages a
test jar which contains a Filter that supresses rows containing "foo".  The
example shows copying the FooFilter.jar into HDFS and then making an Accumulo
table reference that jar.


Execute the following command in the shell.

    $ hadoop fs -copyFromLocal $ACCUMULO_HOME/test/src/test/resources/FooFilter.jar /user1/lib

Execute following in Accumulo shell to setup classpath context

    root@test15> config -s general.vfs.context.classpath.cx1=hdfs://<namenode host>:<namenode port>/user1/lib

Create a table

    root@test15> createtable nofoo

The following command makes this table use the configured classpath context

    root@test15 nofoo> config -t nofoo -s table.classpath.context=cx1

The following command configures an iterator thats in FooFilter.jar

    root@test15 nofoo> setiter -n foofilter -p 10 -scan -minc -majc -class org.apache.accumulo.test.FooFilter
    Filter accepts or rejects each Key/Value pair
    ----------> set FooFilter parameter negate, default false keeps k/v that pass accept method, true rejects k/v that pass accept method: false

The commands below show the filter is working.

    root@test15 nofoo> insert foo1 f1 q1 v1
    root@test15 nofoo> insert noo1 f1 q1 v2
    root@test15 nofoo> scan
    noo1 f1:q1 []    v2
    root@test15 nofoo> 

Below, an attempt is made to add the FooFilter to a table thats not configured
to use the clasppath context cx1.  This fails util the table is configured to
use cx1.

    root@test15 nofoo> createtable nofootwo
    root@test15 nofootwo> setiter -n foofilter -p 10 -scan -minc -majc -class org.apache.accumulo.test.FooFilter
    2013-05-03 12:49:35,943 [shell.Shell] ERROR: java.lang.IllegalArgumentException: org.apache.accumulo.test.FooFilter
    root@test15 nofootwo> config -t nofootwo -s table.classpath.context=cx1
    root@test15 nofootwo> setiter -n foofilter -p 10 -scan -minc -majc -class org.apache.accumulo.test.FooFilter
    Filter accepts or rejects each Key/Value pair
    ----------> set FooFilter parameter negate, default false keeps k/v that pass accept method, true rejects k/v that pass accept method: false


