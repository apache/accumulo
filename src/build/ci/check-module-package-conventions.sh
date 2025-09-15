#! /usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# The purpose of this ci script is to ensure that a pull request
# doesn't unintentionally add a jar resource that would cause
# problems with jar sealing by breaking our package naming
# conventions, which are to use package names based on the
# module, so they are unique.
NUM_EXPECTED=0
ALLOWED=(
  # test module uses main path for ITs, so log4j2-test.properties is okay there
  test/src/main/resources/log4j2-test.properties

  # special exceptions for the main assembly resources; these can be at the root
  assemble/src/main/resources/LICENSE
  assemble/src/main/resources/NOTICE

  # special exceptions for the native tarball assembly resources; these can be at the root
  server/native/src/main/resources/LICENSE
  server/native/src/main/resources/Makefile
  server/native/src/main/resources/NOTICE

  # TODO: these test classes should be moved into the correct package for the module
  start/src/test/java/test/HelloWorldTemplate
  start/src/test/java/test/TestTemplate
  start/src/test/java/test/Test.java
  test/src/main/java/org/apache/accumulo/harness/conf/AccumuloClusterConfiguration.java
  test/src/main/java/org/apache/accumulo/harness/conf/AccumuloMiniClusterConfiguration.java
  test/src/main/java/org/apache/accumulo/harness/conf/AccumuloClusterPropertyConfiguration.java
  test/src/main/java/org/apache/accumulo/harness/conf/StandaloneAccumuloClusterConfiguration.java
  test/src/main/java/org/apache/accumulo/harness/Timeout.java
  test/src/main/java/org/apache/accumulo/harness/WithTestNames.java
  test/src/main/java/org/apache/accumulo/harness/AccumuloClusterHarness.java
  test/src/main/java/org/apache/accumulo/harness/AccumuloITBase.java
  test/src/main/java/org/apache/accumulo/harness/MiniClusterConfigurationCallback.java
  test/src/main/java/org/apache/accumulo/harness/MiniClusterHarness.java
  test/src/main/java/org/apache/accumulo/harness/SharedMiniClusterBase.java
  test/src/main/java/org/apache/accumulo/harness/TestingKdc.java

  # TODO: these test resources should be moved into the correct package for the module
  core/src/test/resources/accumulo.jceks
  core/src/test/resources/empty.jceks
  core/src/test/resources/site-cfg.jceks
  core/src/test/resources/accumulo2.properties
  core/src/test/resources/accumulo3.properties
  core/src/test/resources/passwords.jceks
  minicluster/src/test/resources/FooFilter.jar
  server/tserver/src/test/resources/walog-from-15.walog
  server/tserver/src/test/resources/walog-from-16.walog
  server/tserver/src/test/resources/walog-from-14/550e8400-e29b-41d4-a716-446655440000
  server/tserver/src/test/resources/walog-from-20.walog
  test/src/main/resources/v2_import_test/README.md
  test/src/main/resources/v2_import_test/data/A0000008.rf
  test/src/main/resources/v2_import_test/data/A0000009.rf
  test/src/main/resources/v2_import_test/data/A000000a.rf
  test/src/main/resources/v2_import_test/data/A000000b.rf
  test/src/main/resources/v2_import_test/data/distcp.txt
  test/src/main/resources/v2_import_test/data/exportMetadata.zip

  # TODO: these minicluster classes should be moved into the correct package for the module
  minicluster/src/main/java/org/apache/accumulo/cluster/standalone/StandaloneAccumuloCluster.java
  minicluster/src/main/java/org/apache/accumulo/cluster/standalone/StandaloneClusterControl.java
  minicluster/src/main/java/org/apache/accumulo/cluster/ClusterUsers.java
  minicluster/src/main/java/org/apache/accumulo/cluster/ClusterUser.java
  minicluster/src/main/java/org/apache/accumulo/cluster/RemoteShell.java
  minicluster/src/main/java/org/apache/accumulo/cluster/AccumuloCluster.java
  minicluster/src/main/java/org/apache/accumulo/cluster/ClusterControl.java
  minicluster/src/main/java/org/apache/accumulo/cluster/RemoteShellOptions.java
  minicluster/src/test/java/org/apache/accumulo/cluster/standalone/StandaloneAccumuloClusterTest.java
  minicluster/src/test/java/org/apache/accumulo/cluster/standalone/StandaloneClusterControlTest.java
)

ALLOWED_PIPE_SEP=$({ for x in "${ALLOWED[@]}"; do echo "$x"; done; } | paste -sd'|')

function findwrongpackagesinmodules() {
  local modulepom
  local moduledir
  local packagename
  local findargs
  shopt -s globstar
  for modulepom in **/pom.xml; do
    [[ $modulepom =~ ^(target|.*/target)/.*$ ]] && continue
    moduledir=$(dirname "$modulepom")
    [[ $moduledir == '.' ]] && continue
    (
      cd "$moduledir" || exit 1
      packagename=$(basename "$moduledir")
      # some modules have package naming conventions that differ
      # slightly from the module name
      [[ $moduledir == server/base ]] && packagename=server
      [[ $moduledir == iterator-test-harness ]] && packagename=iteratortest
      [[ $moduledir == hadoop-mapreduce ]] && packagename=hadoop
      findargs=(
        # allow any that use the expected package name, optionally ending with Impl
        -not -regex '^src/\(main\|test\)/\(java\|resources\)/org/apache/accumulo/'"${packagename}"'\(Impl\)?/.*'
        # some test resources are okay at the root
        -not -regex '^src/test/resources/\(log4j2-test\|accumulo\)[.]properties$'
      )
      find src/{main,test}/{java,resources} -type f "${findargs[@]}" 2>/dev/null
    ) | xargs -I '{}' echo "$moduledir/{}"
  done | grep -Pv "^(${ALLOWED_PIPE_SEP//./[.]})\$"
}

function comparecounts() {
  local count
  count=$(findwrongpackagesinmodules | wc -l)
  if [[ $NUM_EXPECTED -ne $count ]]; then
    echo "Expected $NUM_EXPECTED, but found $count unapproved package names in the maven modules:"
    findwrongpackagesinmodules 'print'
    return 1
  fi
}

comparecounts && echo "Found exactly $NUM_EXPECTED unapproved package names in the maven modules"
