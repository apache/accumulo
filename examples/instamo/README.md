Instamo
=======

Introduction
-----------

Instamo makes it easy to write some code and run it against a local, transient
[Accumulo](http://accumulo.apache.org) instance in minutes.  No setup or
installation is required.  This is possible if Java and Maven are already
installed by following the steps below.

```
vim src/main/java/org/apache/accumulo/instamo/AccumuloApp.java
mvn package
```

Map Reduce
----------

Its possible to run local map reduce jobs against the MiniAccumuloCluster
instance.   There is an example of this in the src directory  The following
command will run the map reduce example.

```
mvn exec:exec
```

