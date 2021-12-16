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

testsPerJob=15
if [[ -n $1 && $1 =~ ^[0-9]*$ ]]; then
  testsPerJob=$1
fi
echo "Creating matrix (tests per job: $testsPerJob)..."

gitRootDir=$(git rev-parse --show-toplevel)
# this only works because our test paths don't have spaces; we should keep it that way
count=0
echo -n '::set-output name=matrix::{"profile":['
for x in $(find "$gitRootDir" -name '*IT.java' -exec basename '{}' .java \; | sort -u | xargs -n "$testsPerJob" | tr ' ' ','); do
  [[ $count -gt 0 ]] && echo -n ','
  echo -n "{\"name\":\"task_$count\",\"its\":\"$x\"}"
  ((count=count+1))
done
echo ']}'
echo "Finished creating matrix ($count tasks)"

