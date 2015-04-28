#!/usr/bin/env bash

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

# 
# Extract the native map tarball and automatically build the native library shared object
#
# Exit codes:
#   1 = Failed to create a temp dir in target
#   2 = Failed to run build_native_library.sh
#   3 = Failed to find libaccumulo.so after successful build_native_library.sh
#   4 = Target doesn't exist (invoke build)
#   5 = Accumulo tarball doesn't exist (invoke assembly profile)
#

LIBRARY_NAME="libaccumulo.so"

if [ "$(uname)" == 'Darwin' ]; then
  LIBRARY_NAME='libaccumulo.dylib'
fi

TARGET="$(pwd)/target"

test -d ${TARGET} || exit 4

# Make a temp dir in target
TMP_DIR=$(mktemp -d ${TARGET}/accumulo-native.XXXX) || exit 1

# Find the tarball 
TARBALL=$(find ${TARGET} -name 'accumulo-*.tar.gz' | head -n1)

if [ -z "${TARBALL}" ]; then
  echo "Could not find Accumulo tarball. Did you invoke assemble profile?"
  exit 5
fi

echo "Building native library from ${TARBALL}"

# Extract Accumulo tarball into temp directory in target/
tar xf ${TARBALL} -C ${TMP_DIR}

# Get the directory of the unpacked tarball
ACCUMULO_DIR=$(find "${TMP_DIR}" -maxdepth 1 -mindepth 1 -type d)

# Move into the extracted Accumulo tarball
pushd ${ACCUMULO_DIR}

# Build the native library the normal way
./bin/build_native_library.sh || exit 2

# Make sure the native library is there
test -e lib/native/${LIBRARY_NAME} || exit 3

# Copy the file to the root of target
cp lib/native/${LIBRARY_NAME} ${TARGET}

echo "Created ${TARGET}/${LIBRARY_NAME}"
