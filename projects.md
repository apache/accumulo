---
title: Related Projects
---

The Apache Accumulo community is happy to promote and encourage use of Accumulo in ways that are novel and reusable
by other users within the community. As such, we're happy to curate a list of projects related to Accumulo to give
them visibility to a larger audience. To have you project listed here, send a request to the
[developer's mailing list](mailto:dev@accumulo.apache.org)

## Open source projects using Accumulo

#### Fluo

[Fluo](https://fluo.apache.org) builds on Accumulo and enables low latency, continuous incremental processing of big data.

#### Geomesa

[Geomesa](http://www.geomesa.org/) is an open-source, distributed, spatio-temporal database built on a number of distributed cloud data storage systems, including Accumulo, HBase, Cassandra, and Kafka.

#### Geowave

[Geowave](https://ngageoint.github.io/geowave/) is a library for storage, index, and search of multi-dimensional data on top of a sorted key-value datastore. 

#### Gora

[Gora](https://gora.apache.org/) open source framework provides an in-memory data model and persistence for big data.  Accumulo's continuous ingest test suite was adapted to Gora and called [Goraci](http://gora.apache.org/current/index.html#goraci-integration-testsing-suite).

#### Graphulo

Graphulo is a Java library for Apache Accumulo which delivers server-side sparse matrix math primitives that
enable higher-level graph algorithms and analytics. [Code](https://github.com/Accla/graphulo).

#### Hive

[Hive](https://hive.apache.org/) data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL.
Hive has the ability to read and write data in Accumulo using the [AccumuloStorageHandler](https://cwiki.apache.org/confluence/display/Hive/AccumuloIntegration).

#### Pig

[Pig](http://pig.apache.org/) is a platform for analyzing large data sets that consists of a high-level language for expressing data analysis programs, coupled with infrastructure for evaluating these programs.  Pig has the ability to read and write data in Accumulo using [AccumuloStorage](http://pig.apache.org/docs/r0.16.0/func.html#AccumuloStorage).

#### Presto

[Presto](https://prestodb.io/) is an open source distributed SQL query engine for running interactive analytic queries against data sources of all sizes, ranging from gigabytes to petabytes.  Through the use of the new Accumulo connector for Presto, users are able to execute traditional SQL queries against new and existing tables in Accumulo.  For more information, see the [Accumulo Connector](https://prestodb.io/docs/current/connector/accumulo.html) documentation.

#### Rya

[Rya](http://rya.apache.org/) is a scalable RDF triple store built on top of a columnar index store.

#### Timely

[Timely](https://nationalsecurityagency.github.io/timely/) : A secure time series database based on Accumulo and Grafana.

#### Uno and Muchos

[Uno](https://github.com/astralway/uno) and [Muchos](https://github.com/astralway/muchos) provide automation for quickly setting up Accumulo instances for testing.  These project were created to enable Fluo testing, but can be used to setup just Accumulo.

## User-Created Applications

#### Trendulo

[Trendulo](http://trendulo.com/) is Twitter trend analysis using Apache Accumulo. The [source code](https://github.com/jaredwinick/Trendulo) is publicly available.

#### Wikisearch 

The [Wikisearch project]({{ site.baseurl }}/example/wikisearch) is a rough example of generalized secondary indexing, both ingest
and search, built on top of Apache Accumulo. This write contains more information on the project as well as some
general performance numbers of the project.

#### Node.js, RabbitMQ, and Accumulo

A [simple example](https://github.com/joshelser/node-accumulo) using Node.js to interact with Accumulo using RabbitMQ .

#### ProxyInstance

ProxyInstance is a Java Instance implementation of the Accumulo Instance interface that communicates with
an Accumulo cluster via Accumulo's Apache Thrift proxy server. [Documentation](https://jhuapl.github.io/accumulo-proxy-instance/proxy_instance_user_manual) and
[code](https://github.com/JHUAPL/accumulo-proxy-instance) are available.

## Github

[Github](https://github.com/search?q=accumulo&type=Repositories) also contains many projects that use/reference Accumulo
in some way, shape or form.
