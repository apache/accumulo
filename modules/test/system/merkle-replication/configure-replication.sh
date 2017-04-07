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

source_commands="${tmpdir}/source_commands.txt"

echo 'Removing old tables and setting replication name on source'

echo "deletetable -f $SOURCE_TABLE_NAME" >> $source_commands
echo "createtable $SOURCE_TABLE_NAME" >> $source_commands
echo "config -s replication.name=source" >> $source_commands
echo "quit" >> $source_commands

# Source: drop and create tables, configure unique name for replication and grant perms
echo $SOURCE_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $SOURCE_ACCUMULO_USER -z \
    $SOURCE_INSTANCE $SOURCE_ZOOKEEPERS -f $source_commands

destination_commands="${tmpdir}/destination_commands.txt"

echo 'Removing old tables and setting replication name on destination'

echo "deletetable -f $DESTINATION_TABLE_NAME" >> $destination_commands
echo "createtable $DESTINATION_TABLE_NAME" >> $destination_commands
echo "config -s replication.name=destination" >> $destination_commands
echo "quit" >> $destination_commands

# Destination: drop and create tables, configure unique name for replication and grant perms
echo $DESTINATION_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $DESTINATION_ACCUMULO_USER -z \
    $DESTINATION_INSTANCE $DESTINATION_ZOOKEEPERS -f $destination_commands

rm $source_commands
rm $destination_commands

table_id=$(echo $DESTINATION_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $DESTINATION_ACCUMULO_USER -z \
    $DESTINATION_INSTANCE $DESTINATION_ZOOKEEPERS -e 'tables -l' | grep "${DESTINATION_TABLE_NAME}" \
    | grep -v "${DESTINATION_MERKLE_TABLE_NAME}" | awk '{print $3}')

echo "Configuring $SOURCE_TABLE_NAME to replicate to $DESTINATION_TABLE_NAME (id=$table_id)"

# Define our peer 'destination' with the ReplicaSystem impl, instance name and ZKs
echo "config -s replication.peer.destination=org.apache.accumulo.tserver.replication.AccumuloReplicaSystem,$DESTINATION_INSTANCE,$DESTINATION_ZOOKEEPERS" >> $source_commands
# Username for 'destination'
echo "config -s replication.peer.user.destination=$DESTINATION_ACCUMULO_USER" >> $source_commands
# Password for 'destination'
echo "config -s replication.peer.password.destination=$DESTINATION_ACCUMULO_PASSWORD" >> $source_commands
# Configure replication to 'destination' for $SOURCE_TABLE_NAME
echo "config -t $SOURCE_TABLE_NAME -s table.replication.target.destination=$table_id" >> $source_commands
# Enable replication for the table
echo "config -t $SOURCE_TABLE_NAME -s table.replication=true" >> $source_commands
echo "quit" >> $source_commands

# Configure replication from source to destination and then enable it
echo $SOURCE_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $SOURCE_ACCUMULO_USER -z \
    $SOURCE_INSTANCE $SOURCE_ZOOKEEPERS -f $source_commands

rm $source_commands

# Add some splits to make ingest faster
echo 'Adding splits...'

echo $SOURCE_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $SOURCE_ACCUMULO_USER -z \
    $SOURCE_INSTANCE $SOURCE_ZOOKEEPERS -e "addsplits -t $SOURCE_TABLE_NAME 1 2 3 4 5 6 7 8 9"

echo $DESTINATION_ACCUMULO_PASSWORD | ${ACCUMULO_HOME}/bin/accumulo shell -u $DESTINATION_ACCUMULO_USER -z \
    $DESTINATION_INSTANCE $DESTINATION_ZOOKEEPERS -e "addsplits -t $DESTINATION_TABLE_NAME 1 2 3 4 5 6 7 8 9"

