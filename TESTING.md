<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Testing Apache Accumulo

This document is meant to serve as a quick reference to the automated test suites included in Apache Accumulo for users
to run which validate the product and developers to continue to iterate upon to ensure that the product is stable and as
free of bugs as possible.

The automated testing suite can be categorized as two sets of tests: unit tests and integration tests. These are the
traditional unit and integrations tests as defined by the Apache Maven [lifecycle][3] phases.

# Unit tests

Unit tests can be run by invoking `mvn test` at the root of the Apache Accumulo source tree.  For more information see
the [maven-surefire-plugin docs][4].

The unit tests should run rather quickly (order of minutes for the entire project) and, in nearly all cases, do not
require any noticable amount of computer resources (the compilation of the files typically exceeds running the tests).
Maven will automatically generate a report for each unit test run and will give a summary at the end of each Maven
module for the total run/failed/errored/skipped tests.

The Apache Accumulo developers expect that these tests are always passing on every revision of the code. If this is not
the case, it is almost certainly in error.

# Integration tests

Integration tests can be run by invoking `mvn verify` at the root of the Apache Accumulo source tree.  For more
information see the [maven-failsafe-plugin docs][5].

The integration tests are medium length tests (order minutes for each test class and order hours for the complete suite)
but are checking for regressions that were previously seen in the codebase. These tests do require a noticable amount of
resources, at least another gigabyte of memory over what Maven itself requires. As such, it's recommended to have at
least 3-4GB of free memory and 10GB of free disk space.

## Accumulo for testing

The primary reason these tests take so much longer than the unit tests is that most are using an Accumulo instance to
perform the test. It's a necessary evil; however, there are things we can do to improve this.

## MiniAccumuloCluster

By default, these tests will use a MiniAccumuloCluster which is a multi-process "implementation" of Accumulo, managed
through Java interfaces. This MiniAccumuloCluster has the ability to use the local filesystem or Apache Hadoop's
MiniDFSCluster, as well as starting one to many tablet servers. MiniAccumuloCluster tends to be a very useful tool in
that it can automatically provide a workable instance that mimics how an actual deployment functions.

The downside of using MiniAccumuloCluster is that a significant portion of each test is now devoted to starting and
stopping the MiniAccumuloCluster.  While this is a surefire way to isolate tests from interferring with one another, it
increases the actual runtime of the test by, on average, 10x.

## Standalone Cluster

An alternative to the MiniAccumuloCluster for testing, a standalone Accumulo cluster can also be configured for use by
most tests. This requires a manual step of building and deploying the Accumulo cluster by hand. The build can then be
configured to use this cluster instead of always starting a MiniAccumuloCluster.  Not all of the integration tests are
good candidates to run against a standalone Accumulo cluster, these tests will still launch a MiniAccumuloCluster for
their use.

Use of a standalone cluster can be enabled using system properties on the Maven command line or, more concisely, by
providing a Java properties file on the Maven command line. The use of a properties file is recommended since it is
typically a fixed file per standalone cluster you want to run the tests against.

### Configuration

The following properties can be used to configure a standalone cluster:

- `accumulo.it.cluster.type`, Required: The type of cluster is being defined (valid options: MINI and STANDALONE)
- `accumulo.it.cluster.standalone.admin.principal`, Required: Standalone cluster principal (user) with all System permissions
- `accumulo.it.cluster.standalone.admin.password`, Required: Password for the principal (only valid w/o Kerberos)
- `accumulo.it.cluster.standalone.admin.keytab`, Required: Keytab for the principal (only valid w/ Kerberos)
- `accumulo.it.cluster.standalone.zookeepers`, Required: ZooKeeper quorum used by the standalone cluster
- `accumulo.it.cluster.standalone.instance.name`, Required: Accumulo instance name for the cluster
- `accumulo.it.cluster.standalone.hadoop.conf`, Required: `HADOOP_CONF_DIR`
- `accumulo.it.cluster.standalone.home`, Optional: `ACCUMULO_HOME`
- `accumulo.it.cluster.standalone.conf`, Optional: `ACCUMULO_CONF_DIR`
- `accumulo.it.cluster.standalone.server.user`, Optional: The user Accumulo is running as (used to sudo when starting/stopping Accumulo). Default "accumulo"

Additionally, when running with Kerberos enabled, it is required that Kerberos principals already exist
for the tests to use. As such, a number of properties exist to allow users to be passed down for tests
to use. When Kerberos is enabled, these are principal/username and a path to a keytab file pairs. For "unsecure"
installations, these are just principal/username and password pairs. It is not required to create the users
in Accumulo -- the provided admin user will be used to create the user accounts in Accumulo when necessary.

Setting 5 users should be sufficient for all of the integration test's purposes. Each property is suffixed
with an integer which groups the keytab or password with the username.

- `accumulo.it.cluster.standalone.users.$x` The principal name
- `accumulo.it.cluster.standalone.passwords.$x` The password for the user
- `accumulo.it.cluster.standalone.keytabs.$x` The path to the keytab for the user

Each of the above properties can be set on the commandline (-Daccumulo.it.cluster.standalone.principal=root), or the
collection can be placed into a properties file and referenced using "accumulo.it.cluster.properties". Properties
specified on the command line override properties set in a file.  For example, the following might be similar to
what is executed for a standalone cluster.

  `mvn verify -Daccumulo.it.properties=/home/user/my_cluster.properties`

For the optional properties, each of them will be extracted from the environment if not explicitly provided.
Specifically, `ACCUMULO_HOME` and `ACCUMULO_CONF_DIR` are used to ensure the correct version of the bundled
Accumulo scripts are invoked and, in the event that multiple Accumulo processes exist on the same physical machine,
but for different instances, the correct version is terminated. `HADOOP_CONF_DIR` is used to ensure that the necessary
files to construct the FileSystem object for the cluster can be constructed (e.g. core-site.xml and hdfs-site.xml),
which is typically required to interact with HDFS.

# Manual Distributed Testing

Apache Accumulo also contains a number of tests which are suitable for running against large clusters for hours to days
at a time, for example the [Continuous Ingest][1] and [Randomwalk test][2] suites. These all exist in the repository under
`test/system` and contain their own README files for configuration and use.

[1]: test/system/continuous/README.md
[2]: test/system/randomwalk/README.md
[3]: https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html
[4]: http://maven.apache.org/surefire/maven-surefire-plugin/
[5]: http://maven.apache.org/surefire/maven-failsafe-plugin/

