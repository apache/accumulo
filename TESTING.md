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

## Test Categories

Accumulo uses JUnit Category annotations to categorize certain integration tests based on their runtime requirements.
Presently there are several different categories:

### SunnyDay (`SunnyDayTests`)

This test category represents a minimal set of tests chosen to verify the basic
functionality of Accumulo. These would typically be run prior to submitting a
patch or pull request, or fixing a bug, to quickly ensure no basic functions
were broken by the change.

These tests will run by default during the `integration-test` lifecycle phase using `mvn verify`.
To execute only these tests, use `mvn verify -Dfailsafe.groups=org.apache.accumulo.test.categories.SunnyDayTests`
To execute everything except these tests, use `mvn verify -Dfailsafe.excludedGroups=org.apache.accumulo.test.categories.SunnyDayTests`

### MiniAccumuloCluster (`MiniClusterOnlyTests`)

These tests use MiniAccumuloCluster (MAC) which is a multi-process "implementation" of Accumulo, managed
through Java APIs. This MiniAccumuloCluster has the ability to use the local filesystem or Apache Hadoop's
MiniDFSCluster, as well as starting one to many tablet servers. MiniAccumuloCluster tends to be a very useful tool in
that it can automatically provide a workable instance that mimics how an actual deployment functions.

The downside of using MiniAccumuloCluster is that a significant portion of each test is now devoted to starting and
stopping the MiniAccumuloCluster.  While this is a surefire way to isolate tests from interferring with one another, it
increases the actual runtime of the test by, on average, 10x. Some times the tests require the use of MAC because the
test is being destructive or some special environment setup (e.g. Kerberos).

These tests will run by default during the `integration-test` lifecycle phase using `mvn verify`.
To execute only these tests, use `mvn verify -Dfailsafe.groups=org.apache.accumulo.test.categories.MiniClusterOnlyTests`
To execute everything except these tests, use `mvn verify -Dfailsafe.excludedGroups=org.apache.accumulo.test.categories.MiniClusterOnlyTests`

### Standalone Cluster (`StandaloneCapableClusterTests`)

An alternative to the MiniAccumuloCluster for testing, a standalone Accumulo cluster can also be configured for use by
most tests. This requires a manual step of building and deploying the Accumulo cluster by hand. The build can then be
configured to use this cluster instead of always starting a MiniAccumuloCluster.  Not all of the integration tests are
good candidates to run against a standalone Accumulo cluster, these tests will still launch a MiniAccumuloCluster for
their use.

Use of a standalone cluster can be enabled using system properties on the Maven command line or, more concisely, by
providing a Java properties file on the Maven command line. The use of a properties file is recommended since it is
typically a fixed file per standalone cluster you want to run the tests against.

These tests will run by default during the `integration-test` lifecycle phase using `mvn verify`.
To execute only these tests, use `mvn verify -Dfailsafe.groups=org.apache.accumulo.test.categories.StandaloneCapableClusterTests`
To execute everything except these tests, use `mvn verify -Dfailsafe.excludedGroups=org.apache.accumulo.test.categories.StandaloneCapableClusterTests`

### Performance Tests (`PerformanceTests`)

This category of tests refer to integration tests written specifically to
exercise expected performance, which may be dependent on the available
resources of the host machine. Normal integration tests should be capable of
running anywhere with a lower-bound on available memory.

These tests will run by default during the `integration-test` lifecycle phase using `mvn verify`.
To execute only these tests, use `mvn verify -Dfailsafe.groups=org.apache.accumulo.test.categories.PerformanceTests`
To execute everything except these tests, use `mvn verify -Dfailsafe.excludedGroups=org.apache.accumulo.test.categories.PerformanceTests`

## Configuration for Standalone clusters

The following properties can be used to configure a standalone cluster:

- `accumulo.it.cluster.type`, Required: The type of cluster is being defined (valid options: MINI and STANDALONE)
- `accumulo.it.cluster.standalone.admin.principal`, Required: Standalone cluster principal (user) with all System permissions
- `accumulo.it.cluster.standalone.admin.password`, Required: Password for the principal (only valid w/o Kerberos)
- `accumulo.it.cluster.standalone.admin.keytab`, Required: Keytab for the principal (only valid w/ Kerberos)
- `accumulo.it.cluster.standalone.zookeepers`, Required: ZooKeeper quorum used by the standalone cluster
- `accumulo.it.cluster.standalone.instance.name`, Required: Accumulo instance name for the cluster
- `accumulo.it.cluster.standalone.hadoop.conf`, Required: Hadoop configuration directory
- `accumulo.it.cluster.standalone.home`, Required: Accumulo installation directory on cluster
- `accumulo.it.cluster.standalone.client.conf`, Required: Accumulo conf directory on client
- `accumulo.it.cluster.standalone.server.conf`, Required: Accumulo conf directory on server
- `accumulo.it.cluster.standalone.client.cmd.prefix`, Optional: Prefix that will be added to Accumulo client commands
- `accumulo.it.cluster.standalone.server.cmd.prefix`, Optional: Prefix that will be added to Accumulo service commands

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

## MapReduce job for Integration tests

[ACCUMULO-3871][6] (re)introduced the ability to parallelize the execution of the Integration Test suite by the use
of MapReduce/YARN. When a YARN cluster is available, this can drastically reduce the amount of time to run all tests.

To run the tests, you first need a list of the tests. A simple way to get a list, is to scan the accumulo-test jar file for them.

`jar -tf lib/accumulo-test.jar | grep IT.class | tr / . | sed -e 's/.class$//' >accumulo-integration-tests.txt`

Then, put the list of files into HDFS:

`hdfs dfs -mkdir /tmp`
`hdfs dfs -put accumulo-integration-tests.txt /tmp/tests`

Finally, launch the job, providing the list of tests to run and a location to store the test results. Optionally, a built
native library shared object can be provided to the Mapper's classpath to enable MiniAccumuloCluster to use the native maps
instead of the Java-based implementation. (Note that the below paths are the JAR and shared object are based on an installation.
These files do exist in the build tree, but at different locations)

`yarn jar lib/accumulo-test.jar org.apache.accumulo.test.mrit.IntegrationtestMapReduce -libjars lib/native/libaccumulo.so /tmp/accumulo-integration-tests.txt /tmp/accumulo-integration-test-results`

# Manual Distributed Testing

Apache Accumulo has a number of tests which are suitable for running against large clusters for hours to days at a time.
These test suites exist in the [accumulo-testing repo][2].

[2]: https://github.com/apache/accumulo-testing
[3]: https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html
[4]: http://maven.apache.org/surefire/maven-surefire-plugin/
[5]: http://maven.apache.org/surefire/maven-failsafe-plugin/
[6]: https://issues.apache.org/jira/browse/ACCUMULO-3871
