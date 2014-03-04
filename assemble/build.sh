#! /usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

loc=$(dirname "$0")
loc=$(cd "$loc/.."; pwd)

cd "$loc"

fail() {
  echo '   ' "$@"
  exit 1
}

run() {
  echo "$@"
  eval "$@"
  if [[ $? != 0 ]] ; then
    fail "$@" fails
  fi
}

runAt() {
  ( cd "$1" ; echo "in $(pwd)"; shift ; run "$@" ) || fail
}

cacheGPG() {
  # make sure gpg agent has key cached
  # TODO prompt for key instead of using default?
  TESTFILE="/tmp/${USER}-gpgTestFile-$(date -u +%s).txt"
  touch "${TESTFILE}" && gpg --sign "${TESTFILE}" && rm -f "${TESTFILE}" "${TESTFILE}.gpg"
}

if [[ $1 = '--create-release-candidate' ]]; then
  cacheGPG
  # create a release candidate from a branch
  run mvn clean release:clean release:prepare release:perform
elif [[ $1 = '--seal-jars' ]]; then
  cacheGPG
  # build a tag, but with sealed jars
  run mvn clean install \
   -P apache-release,seal-jars,thrift,assemble,docs
elif [[ $1 = '--test' ]]; then
  cacheGPG
  # build a tag, but with tests
  run mvn clean install \
   -P apache-release,thrift,assemble,docs
else
  fail "Missing one of: --create-release-candidate, --test, --seal-jars"
fi

