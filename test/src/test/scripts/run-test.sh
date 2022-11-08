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

if [[ -z $1 ]]; then
  echo "Usage: $(basename "$0") TestClass1[,TestClass2,TestClass3] ..."
  echo "       $(basename "$0") \"Prefix*IT[,Prefix2*IT]\" ..."
  echo "       $(basename "$0") \"MyIT#method1+method2\" ..."
  exit 1
fi

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [[ -L ${SOURCE} ]]; do # resolve $SOURCE until the file is no longer a symlink
  bin=$(cd -P "$(dirname "${SOURCE}")" && pwd)
  SOURCE=$(readlink "${SOURCE}")
  [[ ${SOURCE} != /* ]] && SOURCE="${bin}/${SOURCE}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin=$(cd -P "$(dirname "${SOURCE}")" && pwd)
# Stop: Resolve Script Directory

cd "$bin/.." || exit 1

tests=$1
shift

# Let the user provide additional maven options (like -Dsurefire.forkCount=2)
mvn verify -Dit.test="$tests" -Dtest=testnamethatdoesntexist -DfailIfNoTests=false "$@"
