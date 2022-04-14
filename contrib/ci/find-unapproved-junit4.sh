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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# The purpose of this ci script is to ensure that a pull request doesn't
# unintentionally add any new non Junit5/Jupiter tests unless they are
# pre-approved on the ALLOWED list
NUM_EXPECTED=0
ALLOWED='CompactionCoordinatorTest.java|CompactorTest.java|AccumuloVFSClassLoaderTest.java'

function findalljunit4tests() {
  # -P for perl matching, -R for recursive, -l for matching files
  local opts='-PRl'
  if [[ $1 == 'print' ]]; then
    # -P for perl matching, -R for recursive, -l for matching files, -H for always showing filenames
    opts='-PRlH'
  fi
  grep "$opts" --include=*.java \
  --exclude={CompactorTest.java,CompactionCoordinatorTest.java,AccumuloVFSClassLoaderTest.java} \
  'org.junit.[^jupiter]'
}

function comparecounts() {
  local count; count=$(findalljunit4tests | wc -l)
  if [[ $NUM_EXPECTED -ne $count ]]; then
    echo "Expected $NUM_EXPECTED, but found $count unapproved non-ASCII characters:"
    findalljunit4tests 'print'
    return 1
  fi
}

comparecounts && echo "Found exactly $NUM_EXPECTED unapproved JUnit4 tests, as expected"
