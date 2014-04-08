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

struct WalEdits {
    1:list<binary> edits
}

struct KeyValues {
    1:list<data.TKeyValue> keyValues
}

exception RemoteReplicationException {
    1:i32 code,
    2:string reason
}

service RemoteReplication {
    void replicateLog(1:i32 remoteTableId, 2:WalEdits data) throws (1:RemoteReplicationException e),
    void replicateKeyValues(1:i32 remoteTableId, 2:KeyValues data) throws (1:RemoteReplicationException e)
}