---
title: Release Guide
nav: nav_releasing
---

## Versioning

Accumulo has adopted [Semantic Versioning][1] and follows their rules and guidelines.

## Testing

Testing for an Accumulo release includes a few steps that a developer may take without a Hadoop cluster and several that require a working cluster. For minor releases, 
the tests which run on a Hadoop cluster are recommended to be completed but are not required. Running even a reduced set of tests against real hardware is always encouraged
even if the full test suite (in breadth of nodes or duration) is not executed. If PMC members do not believe adequate testing was performed for the sake of making the proposed
release, the release should be vetoed via the normal voting process. New major releases are expected to run a full test suite.

### Stand alone
The following steps can be taken without having an underlying cluster. They SHOULD be handled with each Hadoop profile available for a given release version. To activate an alternative profile specify e.g. "-Dhadoop.profile=2" for the Hadoop 2 profile on the Maven commandline. Some older versions of Accumulo referred to Hadoop profiles diferently; see the README that came with said versions for details on building against different Hadoop versions.

  1. All JUnit tests must pass.  This should be a requirement of any patch so it should never be an issue of the codebase.
    - Use "mvn package" to run against the default profile of a particular release
    - Use "mvn -Dhadoop.profile=2 package" to test against the Hadoop 2 profile on e.g. 1.4 or 1.5
    - Use "mvn -Dhadoop.profile=1 package" to test against the Hadoop 1 profile on e.g. 1.6 or later
  - Analyze output of static analysis tools like Findbugs and PMD.
  - For versions 1.6 and later, all functional tests must pass via the Maven failsafe plugin.
    - Use "mvn verify" to run against the default profile of a particular release
    - Use "mvn -Dhadoop.profile=1 verify" to run the functional tests against the Hadoop 1 profile

### Cluster based
The following tests require a Hadoop cluster running a minimum of HDFS, MapReduce, and ZooKeeper. The cluster MAY have any number of worker nodes; it can even be a single node in pseudo-distributed mode. A cluster with multiple tablet servers SHOULD be used so that more of the code base will be exercised. For the purposes of release testing, you should note the number of nodes and versions used. See the Releasing section for more details.

  1. For versions prior to 1.6, all functional tests must complete successfully.
    - See $ACCUMULO_HOME/test/system/auto/README for details on running the functional tests.
  - Two 24-hour periods of the LongClean module of the RandomWalk test need to be run successfully. One of them must use agitation and the other should not.
    - See $ACCUMULO_HOME/test/system/randomwalk/README for details on running the LongClean module.
  - Two 24-hour periods of the continuous ingest test must be validated successfully. One test period must use agitation and the other should not.
    - See $ACCUMULO_HOME/test/system/continuous/README for details on running and verifying the continuous ingest test.
  - Two 72-hour periods of continuous ingest must run. One test period must use agitation and the other should not. No validation is necessary but the cluster should be checked to ensure it is still functional.

## Releasing

  1. Tag the tested branch. It should:
    - Have its version set to note it is RC1.
    - Be fully built, including a tar.gz of the entire project as well as the documentation.
  - PGP Signatures of the tarball must be signed to a separate file and made available to the public, along with the tarball and MD5 and SHA512 checksums. The [Apache Nexus server][nexus] fills this role for us via the maven-release-plugin.
  - A vote must be made on dev@accumulo. Lazy consensus is not sufficient for a release; at least 3 +1 votes from PMC members are required. All checksums and signatures need to be verified before any voter can +1 it. Voting shall last 72 hours.
    - Voters SHOULD include with their vote details on the tests from the testing section they have successfully run. If given, said details for each test MUST include: the number of worker nodes in the cluster, the operating system and version, the Hadoop version, and the Zookeeper version.  For testing done on a version other than the release candidate that is deemed relevant, include the commit hash. All such gathered testing information will be included in the release notes. 
  - Upon successful vote, the new releases can be retagged to remove the RC status and released on the Accumulo webpage.
  - If at any time the tag needs to be remade due to any sort of error, it should be incremented to the next release candidate number.

[1]: {{ site.baseurl }}/versioning
[nexus]: https://repository.apache.org
