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
  # tests in the test module require an accumulo.properties file for test execution
  test/src/main/resources/accumulo.properties

  # special exceptions for the main assembly resources; these can be at the root
  assemble/src/main/resources/LICENSE
  assemble/src/main/resources/NOTICE

  # special exceptions for the native tarball assembly resources; these can be at the root
  server/native/src/main/resources/LICENSE
  server/native/src/main/resources/Makefile
  server/native/src/main/resources/NOTICE
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
