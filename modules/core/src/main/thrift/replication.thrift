/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

namespace java org.apache.accumulo.core.replication.thrift
namespace cpp org.apache.accumulo.core.replication.thrift

include "data.thrift"
include "security.thrift"

struct WalEdits {
    1:list<binary> edits
}

struct KeyValues {
    1:list<data.TKeyValue> keyValues
}

enum RemoteReplicationErrorCode {
  COULD_NOT_DESERIALIZE
  COULD_NOT_APPLY
  TABLE_DOES_NOT_EXIST
  CANNOT_AUTHENTICATE
  CANNOT_INSTANTIATE_REPLAYER
}

enum ReplicationCoordinatorErrorCode {
  NO_AVAILABLE_SERVERS
  SERVICE_CONFIGURATION_UNAVAILABLE
  CANNOT_AUTHENTICATE
}

exception ReplicationCoordinatorException {
    1:ReplicationCoordinatorErrorCode code,
    2:string reason
}

exception RemoteReplicationException {
    1:RemoteReplicationErrorCode code,
    2:string reason
}

service ReplicationCoordinator {
	string getServicerAddress(1:string remoteTableId, 2:security.TCredentials credentials) throws (1:ReplicationCoordinatorException e),
}

service ReplicationServicer {
    i64 replicateLog(1:string remoteTableId, 2:WalEdits data, 3:security.TCredentials credentials) throws (1:RemoteReplicationException e),
    i64 replicateKeyValues(1:string remoteTableId, 2:KeyValues data, 3:security.TCredentials credentials) throws (1:RemoteReplicationException e)
}