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

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [[ -h "${SOURCE}" ]]; do # resolve $SOURCE until the file is no longer a symlink
   dir=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
   SOURCE=$(readlink "${SOURCE}")
   [[ "${SOURCE}" != /* ]] && SOURCE="${dir}/${SOURCE}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
dir=$( cd -P "$( dirname "${SOURCE}" )" && pwd )
script=$( basename "${SOURCE}" )
# Stop: Resolve Script Directory

# Guess at ACCUMULO_HOME and ACCUMULO_CONF_DIR if not already defined
ACCUMULO_HOME=${ACCUMULO_HOME:-"${dir}/../../.."}
ACCUMULO_CONF_DIR=${ACCUMULO_CONF_DIR:-"$ACCUMULO_HOME/conf"}

# Get the configuration values
. ./merkle-env.sh

tmpdir=$(mktemp -dt "$0.XXXXXXXXXX")

splits=${tmpdir}/splits

echo 1 >> $splits
echo 2 >> $splits
echo 3 >> $splits
echo 4 >> $splits
echo 5 >> $splits
echo 6 >> $splits
echo 7 >> $splits
echo 8 >> $splits
echo 9 >> $splits

commands=${tmpdir}/commands

# Generate leaves of merkle trees for source
echo "deletetable -f $SOURCE_MERKLE_TABLE_NAME" >> $commands
echo "createtable $SOURCE_MERKLE_TABLE_NAME" >> $commands
echo "quit" >> $commands

echo $SOURCE_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $SOURCE_ACCUMULO_USER -z \
    $SOURCE_INSTANCE $SOURCE_ZOOKEEPERS -f $commands

echo -e "\nGenerating merkle tree hashes for $SOURCE_TABLE_NAME"

$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.test.replication.merkle.cli.GenerateHashes -t $SOURCE_TABLE_NAME \
    -o $SOURCE_MERKLE_TABLE_NAME -i $SOURCE_INSTANCE -z $SOURCE_ZOOKEEPERS -u $SOURCE_ACCUMULO_USER \
    -p $SOURCE_ACCUMULO_PASSWORD -nt 8 -hash MD5 --splits $splits

rm $commands

# Generate leaves of merkle trees for destination
echo "deletetable -f $DESTINATION_MERKLE_TABLE_NAME" >> $commands
echo "createtable $DESTINATION_MERKLE_TABLE_NAME" >> $commands
echo "quit" >> $commands

echo $DESTINATION_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $DESTINATION_ACCUMULO_USER -z \
    $DESTINATION_INSTANCE $DESTINATION_ZOOKEEPERS -f $commands

echo -e "\nGenerating merkle tree hashes for $DESTINATION_TABLE_NAME"

$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.test.replication.merkle.cli.GenerateHashes -t $DESTINATION_TABLE_NAME \
    -o $DESTINATION_MERKLE_TABLE_NAME -i $DESTINATION_INSTANCE -z $DESTINATION_ZOOKEEPERS -u $DESTINATION_ACCUMULO_USER \
    -p $DESTINATION_ACCUMULO_PASSWORD -nt 8 -hash MD5 --splits $splits

echo -e "\nComputing root hash:"

#Compute root node of merkle tree
$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.test.replication.merkle.cli.ComputeRootHash -t $SOURCE_MERKLE_TABLE_NAME \
    -i $SOURCE_INSTANCE -z $SOURCE_ZOOKEEPERS -u $SOURCE_ACCUMULO_USER -p $SOURCE_ACCUMULO_PASSWORD -hash MD5

$ACCUMULO_HOME/bin/accumulo org.apache.accumulo.test.replication.merkle.cli.ComputeRootHash -t $DESTINATION_MERKLE_TABLE_NAME \
    -i $DESTINATION_INSTANCE -z $DESTINATION_ZOOKEEPERS -u $DESTINATION_ACCUMULO_USER -p $DESTINATION_ACCUMULO_PASSWORD -hash MD5

rm -rf $tmpdir
