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

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [[ -h "$SOURCE" ]]; do # resolve $SOURCE until the file is no longer a symlink
   bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin=$( cd -P "$( dirname "$SOURCE" )" && pwd )
script=$( basename "$SOURCE" )
# Stop: Resolve Script Directory


lib=${bin}/../lib
native_tarball=${lib}/accumulo-native.tar.gz
final_native_target="${lib}/native"

if [[ ! -f $native_tarball ]]; then
    echo "Could not find native code artifact: ${native_tarball}";
    exit 1
fi

# Make the destination for the native library
mkdir -p "${final_native_target}" || exit 1

# Make a directory for us to unpack the native source into
TMP_DIR=$(mktemp -d /tmp/accumulo-native.XXXX) || exit 1

# Unpack the tarball to our temp directory
tar xf "${native_tarball}" -C "${TMP_DIR}"

if [[ $? != 0 ]]; then
    echo "Failed to unpack native tarball to ${TMP_DIR}"
    exit 1
fi

# Move to the first (only) directory in our unpacked tarball
native_dir=$(find "${TMP_DIR}" -maxdepth 1 -mindepth 1 -type d)

cd "${native_dir}" || exit 1

# Make the native library
export USERFLAGS="$@"
make

# Make sure it didn't fail
if [[ $? != 0 ]]; then
    echo "Make failed!"
    exit 1
fi

# "install" the artifact
cp libaccumulo.* "${final_native_target}" || exit 1

# Clean up our temp directory
rm -rf "${TMP_DIR}"

echo "Successfully installed native library"
