<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Testing Apache Accumulo

This document is meant to serve as a quick reference to the automated test suites of Accumulo.

# Unit tests

Unit tests can be run by invoking `mvn package` at the root of the Apache Accumulo source tree, which includes the
`test` phase of the [Maven lifecycle][lifecycle]. The `test` phase cannot be run directly, because not all of Accumulo's
modules are Java artifacts, and therefore will not be resolvable by their sibling modules until they are created in
their `package` phase. To avoid building against stale artifacts from previous builds that may have been published to a
remote server or installed to your local Maven repository, always build with the `package` phase to run the unit
tests. The [maven-surefire-plugin][surefire] is bound to the `test` phase of the Maven lifecycle by default and will run
the JUnit tests. To execute the unit tests, simply build the project by running:

```bash
mvn clean package
```

# SpotBugs (formerly findbugs)

[SpotBugs] will run by default when building Accumulo (unless "-Dspotbugs.skip" is used) and does a thorough static code
analysis of potential bugs.  There is also a security findbugs plugin configured that can be run with this
command:

```bash
mvn clean verify -Psec-bugs -DskipTests
```

# Integration Tests

The integration tests are medium length tests that check for regressions. These tests do require more memory over what 
Maven itself requires. As such, it's recommended to have at least 3-4GB of free memory and 10GB of free disk space.

Accumulo uses JUnit Category annotations to categorize certain integration tests based on their runtime requirements.
The different categories are listed below.  To run a single IT use the following command. This command will run just
the WriteAheadLogIT:

```bash
mvn clean verify -Dit.test=WriteAheadLogIT -Dtest=foo -Dspotbugs.skip
```

## SunnyDay (`SunnyDay`)

This test category represents a minimal set of tests chosen to verify the basic
functionality of Accumulo. These would typically be run prior to submitting a
patch or pull request, or fixing a bug, to quickly ensure no basic functions
were broken by the change.

These tests will run by default during the `integration-test` lifecycle phase using `mvn verify`.
To run all the Sunny day tests, run:

```bash
mvn clean verify -Psunny
```

## MiniAccumuloCluster (`MiniClusterOnly`)

These tests use MiniAccumuloCluster (MAC) which is a multi-process "implementation" of Accumulo, managed
through Java APIs. This MiniAccumuloCluster has the ability to use the local filesystem or Apache Hadoop's
MiniDFSCluster, as well as starting one to many tablet servers. Most tests will be run in the local directory:

```bash
$ACCUMULO_HOME/test/target/mini-tests
```

The downside of using MiniAccumuloCluster is the extra time it takes to start and stop the MAC.

These tests will run by default during the `integration-test` lifecycle phase using `mvn verify`.
To run all the Mini tests, run:
```bash
mvn clean verify -Dspotbugs.skip
```

## Standalone Cluster (`StandaloneCapableCluster`)

A standalone Accumulo cluster can also be configured for use by most tests. Not all the integration tests are good
candidates to run against a standalone cluster, and some of them require classes in the test jar.  Copy the
accumulo-test jar found in `$ACCUMULO_HOME/test/target` into the lib folder of your accumulo instance before running all
the tests.

These tests can be run by providing a system property.  Specific ITs can be run using "-Dit.test" or run all tests using:

```bash
mvn clean verify -Dtest=foo -Daccumulo.it.properties=/home/user/my_cluster.properties -Dfailsafe.groups=StandaloneCapableCluster -Dspotbugs.skip
```

### Configuration for Standalone clusters

The following properties can be used to configure a standalone cluster:

- `accumulo.it.cluster.type`, Required: The type of cluster is being defined (valid options: MINI and STANDALONE)
- `accumulo.it.cluster.clientconf`, Required: Path to accumulo-client.properties
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

Setting 2 users should be sufficient for all of the integration test's purposes. Each property is suffixed
with an integer which groups the keytab or password with the username.

- `accumulo.it.cluster.standalone.users.$x` The principal name
- `accumulo.it.cluster.standalone.passwords.$x` The password for the user
- `accumulo.it.cluster.standalone.keytabs.$x` The path to the keytab for the user

Each of the above properties can be set on the commandline (-Daccumulo.it.cluster.standalone.principal=root), or the
collection can be placed into a properties file and referenced using "accumulo.it.cluster.properties". Properties
specified on the command line override properties set in a file.

# Manual Distributed Testing

Apache Accumulo has a number of tests which are suitable for running against large clusters for hours to days at a time.
These test suites exist in the [accumulo-testing repo][testing].

[testing]: https://github.com/apache/accumulo-testing
[surefire]: https://maven.apache.org/surefire/maven-surefire-plugin
[SpotBugs]: https://spotbugs.github.io
[lifecycle]: https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle
