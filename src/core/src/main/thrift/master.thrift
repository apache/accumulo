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
}

struct RecoveryStatus {
    1:string host
    2:string name
    3:double mapProgress
    4:double reduceProgress
    5:i32 runtime                   // in millis
    6:double copyProgress
}

struct LoggerStatus {
    1:string logger
}

struct TabletServerStatus {
    1:map<string, TableInfo> tableMap
    2:i64 lastContact
    3:string name
    5:double osLoad
    7:i64 holdTime
    8:i64 lookups
    9:set<string> loggers    
}

enum MasterState {
    INITIAL,
    HAVE_LOCK,
    WAIT_FOR_TSERVERS,
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

struct MasterMonitorInfo {
    1:map<string, TableInfo> tableMap
    2:list<TabletServerStatus> tServerInfo
    3:map<string, byte> badTServers
    4:list<RecoveryStatus> recovery
    5:list<LoggerStatus> loggers
    6:MasterState state
    8:MasterGoalState goalState
    7:i32 unassignedTablets
    9:set<string> serversShuttingDown
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
    UNLOAD_ERROR
}

enum TimeType {
  LOGICAL
  MILLIS
}

service MasterClientService extends client.ClientService {

    // table management methods
    void createTable(6:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName, 3:list<binary> splitPoints, 4:map<string, string> aggs, 5:TimeType tt) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void deleteTable(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void renameTable(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string oldTableName, 3:string newTableName) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void offlineTable(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void onlineTable(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void flushTable(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void setTableProperty(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName, 3:string property, 4:string value) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
    void removeTableProperty(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableName, 3:string property) throws (1:security.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)

    // system management methods
    void setMasterGoalState(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:MasterGoalState state) throws (1:security.ThriftSecurityException sec);
    void shutdown(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:bool stopTabletServers) throws (1:security.ThriftSecurityException sec)
    void shutdownTabletServer(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tabletServer) throws (1: security.ThriftSecurityException sec)
    void setSystemProperty(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string property, 3:string value) throws (1:security.ThriftSecurityException sec)
    void removeSystemProperty(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string property) throws (1:security.ThriftSecurityException sec)

    // system monitoring methods
    MasterMonitorInfo getMasterStats(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)
    
    // tablet server reporting
    oneway void reportSplitExtent(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string serverName, 3:TabletSplit split)
    oneway void reportTabletStatus(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string serverName, 3:TabletLoadState status, 4:data.TKeyExtent tablet)
}
