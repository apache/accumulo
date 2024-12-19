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

# This code will generate the FlatBuffers code that is used
# to serialize Micrometer Meter objects into a byte[] inside
# the Thrift response object returned by the MetricServiceHandler
# that is called by the Monitor.

[[ -z $REQUIRED_FB_VERSION ]] && REQUIRED_FB_VERSION='24.3.25'

# Test to see if we have thrift installed
if ! flatc --version 2>/dev/null | grep -qF "${REQUIRED_FB_VERSION}"; then
  echo "****************************************************"
  echo "*** flatc ${REQUIRED_FB_VERSION} is not available, check PATH"
  echo "*** and ensure that 'flatc' is resolvable or install correct"
  echo "*** version. Generated code will not be updated"
  fail "****************************************************"

  # git clone git@github.com:google/flatbuffers.git
  # cd flatbuffers
  # git checkout v24.3.25
  # cmake -G "Unix Makefiles"
  # make -j
  # make test
  # sudo make install
  # See FlatBuffers README and/or https://flatbuffers.dev for more information
fi

# Run this scrip from the accumulo/core directory, for exampe:
#
# src/main/scripts/generate-flatbuffers.sh

flatc --java -o src/main/flatbuffers-gen-java src/main/flatbuffers/metric.fbs

# Be sure to run `mvn package` after generating the files to add the LICENSE headers
