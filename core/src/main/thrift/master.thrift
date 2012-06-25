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
namespace java org.apache.accumulo.core.master.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "cloudtrace.thrift"

struct Compacting {
    1:i32 running
    2:i32 queued
}

struct TableInfo {
    1:i64 recs
    2:i64 recsInMemory
    3:i32 tablets
    4:i32 onlineTablets
    5:double ingestRate
    6:double ingestByteRate
    7:double queryRate
    8:double queryByteRate
    9:Compacting minor;
    10:Compacting major;
    11:Compacting scans;
    12:double scanRate;
}

struct RecoveryStatus {
    2:string name
    5:i32 runtime                   // in millis
    6:double progress
}

struct TabletServerStatus {
    1:map<string, TableInfo> tableMap
    2:i64 lastContact
    3:string name
    5:double osLoad
    7:i64 holdTime
    8:i64 lookups
    10:i64 indexCacheHits    
    11:i64 indexCacheRequest   
    12:i64 dataCacheHits   
    13:i64 dataCacheRequest
    14:list<RecoveryStatus> logSorts
}

enum MasterState {
    INITIAL,
    HAVE_LOCK,
    SAFE_MODE,
    NORMAL, 
    UNLOAD_METADATA_TABLETS, 
    UNLOAD_ROOT_TABLET, 
    STOP
}

enum MasterGoalState {
    CLEAN_STOP,
    SAFE_MODE,
    NORMAL,
}

struct DeadServer {
    1:string server,
    2:i64 lastStatus,
    3:string status,
}

struct MasterMonitorInfo {
    1:map<string, TableInfo> tableMap
    2:list<TabletServerStatus> tServerInfo
    3:map<string, byte> badTServers
    6:MasterState state
    8:MasterGoalState goalState
    7:i32 unassignedTablets
    9:set<string> serversShuttingDown
    10:list<DeadServer> deadTabletServers
}

struct TabletSplit {
    1:data.TKeyExtent oldTablet
    2:list<data.TKeyExtent> newTablets
}

exception RecoveryException {
    1:string why
}

enum TabletLoadState {
    LOADED,
    LOAD_FAILURE,
    UNLOADED,
    UNLOAD_FAILURE_NOT_SERVING,
    UNLOAD_ERROR,
    CHOPPED
}

enum TableOperation {
  CREATE
  CLONE
  DELETE
  RENAME
  ONLINE
  OFFLINE
  MERGE
  DELETE_RANGE
  BULK_IMPORT
  COMPACT
}

service MasterClientService {

    // table management methods
    i64 initiateFlush(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void waitForFlush(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName, 6:binary startRow, 7:binary endRow, 3:i64 flushID, 4:i64 maxLoops) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    
    void setTableProperty(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName, 3:string property, 4:string value) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void removeTableProperty(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName, 3:string property) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)

    // system management methods
    void setMasterGoalState(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:MasterGoalState state) throws (1:security.ThriftSecurityException sec);
    void shutdown(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:bool stopTabletServers) throws (1:security.ThriftSecurityException sec)
    void shutdownTabletServer(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tabletServer, 4:bool force) throws (1: security.ThriftSecurityException sec)
    void setSystemProperty(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string property, 3:string value) throws (1:security.ThriftSecurityException sec)
    void removeSystemProperty(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string property) throws (1:security.ThriftSecurityException sec)

    // system monitoring methods
    MasterMonitorInfo getMasterStats(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)
    
    // tablet server reporting
    oneway void reportSplitExtent(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string serverName, 3:TabletSplit split)
    oneway void reportTabletStatus(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string serverName, 3:TabletLoadState status, 4:data.TKeyExtent tablet)

   //table operations
   i64 beginTableOperation(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)
   void executeTableOperation(7:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:i64 opid, 3:TableOperation op, 4:list<binary> arguments, 5:map<string, string> options, 6:bool autoClean)throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
   string waitForTableOperation(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:i64 opid) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
   void finishTableOperation(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:i64 opid) throws (1:security.ThriftSecurityException sec)
}
