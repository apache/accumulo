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

# The purpose of this ci script is to ensure that a pull request doesn't
# unintentionally add JUnit jupiter APIs to vintage tests, or add vintage
# APIs to new jupiter tests.
NUM_EXPECTED=0
ALLOWED=(
  server/compaction-coordinator/src/test/java/org/apache/accumulo/coordinator/CompactionCoordinatorTest.java
  server/compactor/src/test/java/org/apache/accumulo/compactor/CompactorTest.java
  start/src/test/java/org/apache/accumulo/start/classloader/vfs/AccumuloVFSClassLoaderTest.java
)

ALLOWED_PIPE_SEP=$({ for x in "${ALLOWED[@]}"; do echo "$x"; done; } | paste -sd'|')

function findalljunitproblems() {
  # -P for perl matching, -R for recursive, -l for matching files
  local opts='-PRl'
  if [[ $1 == 'print' ]]; then
    # -P for perl matching, -R for recursive, -l for matching files, -H for always showing filenames
    opts='-PRlH'
  fi
  # find any new classes using something other than the jupiter API, except those allowed
  grep "$opts" --include='*.java' 'org[.]junit[.](?!jupiter)' | grep -Pv "^(${ALLOWED_PIPE_SEP//./[.]})\$"
  # find any uses of the jupiter API in the allowed vintage classes
  grep "$opts" 'org[.]junit[.]jupiter' "${ALLOWED[@]}"
}

function comparecounts() {
  local count
  count=$(findalljunitproblems | wc -l)
  if [[ $NUM_EXPECTED -ne $count ]]; then
    echo "Expected $NUM_EXPECTED, but found $count classes using the wrong JUnit APIs:"
    findalljunitproblems 'print'
    return 1
  fi
}

comparecounts && echo "Found exactly $NUM_EXPECTED unapproved JUnit API uses, as expected"
