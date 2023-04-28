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

# Check that the generated thrift code hasn't changed from
# what is currently checked in to the repository

set -e

thriftdefault="0.16.0"
rootDir=$(git rev-parse --show-toplevel 2>/dev/null) || ver=$thriftdefault
ver=$({ xmllint --shell "$rootDir/pom.xml" <<<'xpath /*[local-name()="project"]/*[local-name()="properties"]/*[local-name()="thrift.version"]/text()' | grep content= | cut -f2 -d=; } 2>/dev/null || echo "$thriftdefault")
ver=${ver%%-*}

echo "Using Thrift $ver"

downloadloc=~/devtools/thrift-"$ver"
url="https://dist.apache.org/repos/dist/dev/accumulo/devtools/thrift-$ver/thrift"

mkdir -p "$downloadloc"

echo "Checking $downloadloc for thrift binary..."

if [[ -f "$downloadloc/thrift" ]]; then
  echo 'Thrift found. No download necessary.'
else
  echo "Downloading from $url ..."
  curl "$url" -o "$downloadloc/thrift"
fi

fulldownloadloc=$(readlink -f "$downloadloc")

echo 'Checking if thrift modified any files...'
(export PATH="$fulldownloadloc:$PATH" && cd core && src/main/scripts/generate-thrift.sh)

if [[ -n $(git status --porcelain --ignored=no) ]]; then
  echo 'Thrift build changed files in worktree:'
  git status --short --ignored=no
  exit 1
else
  echo 'No changes detected.'
fi
