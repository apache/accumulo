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
namespace cpp org.apache.accumulo.core.master.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "trace.thrift"

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
  9:Compacting minors;
  10:Compacting majors;
  11:Compacting scans;
  12:double scanRate;
}

struct RecoveryStatus {
  2:string name
  5:i32 runtime                   // in millis
  6:double progress
}

enum BulkImportState {
   INITIAL
   # master moves the files into the accumulo area
   MOVING
   # tserver examines the index of the file
   PROCESSING
   # tserver assigns the file to tablets
   ASSIGNING
   # tserver incorporates file into tablet
   LOADING
   # master moves error files into the error directory
   COPY_FILES
   # flags and locks removed
   CLEANUP
}

struct BulkImportStatus {
  1:i64 startTime
  2:string filename
  3:BulkImportState state
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
  15:i64 flushs
  16:i64 syncs
  17:list<BulkImportStatus> bulkImports
  18:string version
  19:i64 responseTime
}

enum MasterState {
  INITIAL
  HAVE_LOCK
  SAFE_MODE
  NORMAL
  UNLOAD_METADATA_TABLETS
  UNLOAD_ROOT_TABLET
  STOP
}

enum MasterGoalState {
  CLEAN_STOP
  SAFE_MODE
  NORMAL
}

struct DeadServer {
  1:string server
  2:i64 lastStatus
  3:string status
}

struct MasterMonitorInfo {
  1:map<string, TableInfo> tableMap
  2:list<TabletServerStatus> tServerInfo
  3:map<string, i8> badTServers
  6:MasterState state
  8:MasterGoalState goalState
  7:i32 unassignedTablets
  9:set<string> serversShuttingDown
  10:list<DeadServer> deadTabletServers
  11:list<BulkImportStatus> bulkImports
}

struct TabletSplit {
  1:data.TKeyExtent oldTablet
  2:list<data.TKeyExtent> newTablets
}

exception RecoveryException {
  1:string why
}

enum TabletLoadState {
  LOADED
  LOAD_FAILURE
  UNLOADED
  UNLOAD_FAILURE_NOT_SERVING
  UNLOAD_ERROR
  CHOPPED
}

enum FateOperation {
  TABLE_CREATE
  TABLE_CLONE
  TABLE_DELETE
  TABLE_RENAME
  TABLE_ONLINE
  TABLE_OFFLINE
  TABLE_MERGE
  TABLE_DELETE_RANGE
  TABLE_BULK_IMPORT
  TABLE_COMPACT
  TABLE_IMPORT
  TABLE_EXPORT
  TABLE_CANCEL_COMPACT
  NAMESPACE_CREATE
  NAMESPACE_DELETE
  NAMESPACE_RENAME
}

service FateService {
  // register a fate operation by reserving an opid
  i64 beginFateOperation(2:trace.TInfo tinfo, 1:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)
  // initiate execution of the fate operation; set autoClean to true if not waiting for completion
  void executeFateOperation(7:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:i64 opid, 3:FateOperation op, 4:list<binary> arguments, 5:map<string, string> options, 6:bool autoClean) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)
  // wait for completion of the operation and get the returned exception, if any
  string waitForFateOperation(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:i64 opid) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)
  // clean up fate operation if autoClean was not set, after waiting
  void finishFateOperation(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:i64 opid) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)
}

service MasterClientService extends FateService {

  // table management methods
  i64 initiateFlush(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string tableName) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)
  void waitForFlush(5:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string tableName, 6:binary startRow, 7:binary endRow, 3:i64 flushID, 4:i64 maxLoops) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)

  void setTableProperty(5:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string tableName, 3:string property, 4:string value) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)
  void removeTableProperty(4:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string tableName, 3:string property) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)

  void setNamespaceProperty(5:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string ns, 3:string property, 4:string value) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)
  void removeNamespaceProperty(4:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string ns, 3:string property) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope, 3:client.ThriftNotActiveServiceException tnase)

  // system management methods
  void setMasterGoalState(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:MasterGoalState state) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase);
  void shutdown(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:bool stopTabletServers) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)
  void shutdownTabletServer(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string tabletServer, 4:bool force) throws (1: client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)
  void setSystemProperty(4:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string property, 3:string value) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)
  void removeSystemProperty(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string property) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)

  // system monitoring methods
  MasterMonitorInfo getMasterStats(2:trace.TInfo tinfo, 1:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)
  void waitForBalance(1:trace.TInfo tinfo) throws (1:client.ThriftNotActiveServiceException tnase)

  // tablet server reporting
  oneway void reportSplitExtent(4:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string serverName, 3:TabletSplit split)
  oneway void reportTabletStatus(5:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string serverName, 3:TabletLoadState status, 4:data.TKeyExtent tablet)

  list<string> getActiveTservers(1:trace.TInfo tinfo, 2:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)

  // Delegation token request
  security.TDelegationToken getDelegationToken(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:security.TDelegationTokenConfig cfg) throws (1:client.ThriftSecurityException sec, 2:client.ThriftNotActiveServiceException tnase)

  // Determine when all provided logs are replicated
  bool drainReplicationTable(1:trace.TInfo tfino, 2:security.TCredentials credentials, 3:string tableName, 4:set<string> logsToWatch) throws (1:client.ThriftNotActiveServiceException tnase)
}
