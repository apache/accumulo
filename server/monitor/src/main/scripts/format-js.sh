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

# This script will attempt to format JavaScript files

jsDir='src/main/resources/org/apache/accumulo/monitor/resources/js'

function beautify() {
  set -x
  npx js-beautify@^1.14 -r -q -n -j -s 2 -f "$@"
  { set +x; } 2>/dev/null
}

if hash npx 2>/dev/null; then
  echo "Found 'npx'; formatting JavaScript files (if needed) in place"
  if ! beautify "$jsDir"/*.js; then
    echo "Error formatting files; you may be using an older version of npm/npx"
    echo "Attempting to format files one at a time..."
    for js in "$jsDir"/*.js; do
      beautify "$js" || echo "Error formatting $js"
    done
  fi
else
  echo "Skipping formatting of JavaScript files, since 'npx' is not present"
fi
