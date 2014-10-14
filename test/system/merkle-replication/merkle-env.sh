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

# Random data will be written to this table
SOURCE_TABLE_NAME='replicationSource'
# Then replicated to this table
DESTINATION_TABLE_NAME='replicationDestination'

# Merkle tree to be stored in this table for the source table
SOURCE_MERKLE_TABLE_NAME="${SOURCE_TABLE_NAME}_merkle"
# Merkle tree to be stored in this table for the destination table
DESTINATION_MERKLE_TABLE_NAME="${DESTINATION_TABLE_NAME}_merkle"

# Connection information to Accumulo
SOURCE_ACCUMULO_USER="user"
SOURCE_ACCUMULO_PASSWORD="password"

DESTINATION_ACCUMULO_USER="${SOURCE_ACCUMULO_USER}"
DESTINATION_ACCUMULO_PASSWORD="${SOURCE_ACCUMULO_PASSWORD}"

SOURCE_INSTANCE="accumulo"
DESTINATION_INSTANCE="${SOURCE_INSTANCE}"

SOURCE_ZOOKEEPERS="localhost"
DESTINATION_ZOOKEEPERS="${SOURCE_ZOOKEEPERS}"

# Accumulo user to be configured on the destination instance
#REPLICATION_USER="${ACCUMULO_USER}"
#REPLICATION_PASSWORD="${ACCUMULO_PASSWORD}"

# Control amount and distribution of data written
NUM_RECORDS=100000000
MAX_ROW=1000000
MAX_CF=10
MAX_CQ=100
DELETE_PERCENT=0
