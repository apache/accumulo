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

# The purpose of this ci script is to ensure that a pull request doesn't unintentionally add an
# abstract class ending with 'IT'.
NUM_EXPECTED=0
ALLOWED=(
)

ALLOWED_PIPE_SEP=$({ for x in "${ALLOWED[@]}"; do echo "$x"; done; } | paste -sd'|')

function findallabstractITproblems() {
  # -R for recursive, -P for perl matching, -l for matching files
  local opts='-RPl'
  if [[ $1 == 'print' ]]; then
      # -R for recursive, -P for perl matching, -l for matching files, -H for always showing filenames
      opts='-RPlH'
  fi
  # find any abstract classes ending in 'IT', except those allowed
  grep "$opts" --include='*.java' 'abstract class.+IT ' | grep -Pv "^(${ALLOWED_PIPE_SEP//./[.]})\$"
}

function comparecounts() {
  local count
  count=$(findallabstractITproblems | wc -l)
  if [[ $NUM_EXPECTED -ne $count ]]; then
    echo "Expected $NUM_EXPECTED, but found $count unapproved abstract classes ending in 'IT'"
    findallabstractITproblems 'print'
    return 1
  fi
}

comparecounts && echo "Found exactly $NUM_EXPECTED unapproved abstract classes ending in 'IT', as expected"
