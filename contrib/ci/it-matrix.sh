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

testsPerJob=15
if [[ -n $1 && $1 =~ ^[0-9]*$ ]]; then
  testsPerJob=$1
fi

# set these to /dev/null if they aren't defined in the environment
GITHUB_OUTPUT="${GITHUB_OUTPUT:-/dev/null}"
GITHUB_STEP_SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"

function createTestMatrix() {

  local count=0
  local chunk=$1
  local batch
  local gitRootDir

  { echo "Creating matrix (tests per job: $chunk)..." | tee -a "$GITHUB_STEP_SUMMARY"; } 1>&2

  gitRootDir=$(git rev-parse --show-toplevel)

  # this only works because our test paths don't have spaces; we should keep it that way
  echo -n '{"profile":['
  for batch in $(find "$gitRootDir" -name '*IT.java' -exec basename '{}' .java \; | sort -u | xargs -n "$chunk" | tr ' ' ','); do
    [[ $count -gt 0 ]] && echo -n ','
    echo -n '{"name":"task_'"$count"'","its":"'"$batch"'"}'
    ((count = count + 1))
  done
  echo ']}'

  { echo "Finished creating matrix ($count tasks)" | tee "$GITHUB_STEP_SUMMARY"; } 1>&2
}

echo "CUSTOM_MATRIX=$(createTestMatrix "$testsPerJob")" | tee -a "$GITHUB_OUTPUT"
