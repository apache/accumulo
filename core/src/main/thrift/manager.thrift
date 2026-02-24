/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
namespace java org.apache.accumulo.core.manager.thrift
namespace cpp org.apache.accumulo.core.manager.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"

struct DeadServer {
  1:string server
  2:i64 lastStatus
  3:string status
  4:string resourceGroup
}

struct TabletSplit {
  1:data.TKeyExtent oldTablet
  2:list<data.TKeyExtent> newTablets
}

exception ThriftPropertyException {
  1:string property
  2:string value
  3:string description
}

enum TabletLoadState {
  LOADED
  LOAD_FAILURE
  UNLOADED
  UNLOAD_FAILURE_NOT_SERVING
  UNLOAD_ERROR
}

enum TFateOperation {
  TABLE_CREATE = 0
  TABLE_CLONE = 1
  TABLE_DELETE = 2
  TABLE_RENAME = 3
  TABLE_ONLINE = 4
  TABLE_OFFLINE = 5
  TABLE_MERGE = 6
  TABLE_DELETE_RANGE = 7
  // 8 was bulk v1 that was removed
  TABLE_COMPACT = 9
  TABLE_IMPORT = 10
  TABLE_EXPORT = 11
  TABLE_CANCEL_COMPACT = 12
  NAMESPACE_CREATE = 13
  NAMESPACE_DELETE = 14
  NAMESPACE_RENAME = 15
  TABLE_BULK_IMPORT2 = 16
  TABLE_TABLET_AVAILABILITY = 17
  TABLE_SPLIT = 18
}

enum ManagerState {
  INITIAL
  HAVE_LOCK
  SAFE_MODE
  NORMAL
  UNLOAD_METADATA_TABLETS
  UNLOAD_ROOT_TABLET
  STOP
}

enum ManagerGoalState {
  CLEAN_STOP
  SAFE_MODE
  NORMAL
}

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
  9:Compacting minors
  11:Compacting scans
  12:double scanRate
}

struct RecoveryStatus {
  2:string name
  // in millis
  5:i32 runtime
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
  15:i64 flushs
  16:i64 syncs
  19:string version
  18:i64 responseTime
}

struct ManagerMonitorInfo {
  1:map<string, TableInfo> tableMap
  2:list<TabletServerStatus> tServerInfo
  3:map<string, i8> badTServers
  4:ManagerState state
  5:ManagerGoalState goalState
  6:i32 unassignedTablets
  7:set<string> serversShuttingDown
  8:list<DeadServer> deadTabletServers
}

enum TFateInstanceType {
  META
  USER
}

struct TFateId {
  1:TFateInstanceType type
  2:string txUUIDStr
}

struct TTabletMergeability {
  // Use a flag for "never" instead of something like a -1 delay
  // in case we want to change how we represent never in the future
  1:bool never
  2:i64 delay
}

service FateService {

  // register a fate operation by reserving an opid
  TFateId beginFateOperation(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:TFateInstanceType type
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  // initiate execution of the fate operation; set autoClean to true if not waiting for completion
  void executeFateOperation(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:TFateId opid
    4:TFateOperation op
    5:list<binary> arguments
    6:map<string, string> options
    7:bool autoClean
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  // wait for completion of the operation and get the returned exception, if any
  string waitForFateOperation(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:TFateId opid
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  // clean up fate operation if autoClean was not set, after waiting
  void finishFateOperation(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:TFateId opid
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  // cancel a fate operation
  bool cancelFateOperation(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:TFateId opid
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )
  
}

service ManagerClientService {

  // table management methods
  i64 initiateFlush(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  void waitForFlush(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:binary startRow
    5:binary endRow
    6:i64 flushID
    7:i64 maxLoops
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  void setTableProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:string property
    5:string value
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
    4:ThriftPropertyException tpe
  )

  void modifyTableProperties(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:client.TVersionedProperties vProperties
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
    4:client.ThriftConcurrentModificationException tcme
    5:ThriftPropertyException tpe
  )

  void removeTableProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:string property
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  void setNamespaceProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
    4:string property
    5:string value
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
    4:ThriftPropertyException tpe
  )

  void modifyNamespaceProperties(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
    4:client.TVersionedProperties vProperties
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
    4:client.ThriftConcurrentModificationException tcme
    5:ThriftPropertyException tpe
  )

  void removeNamespaceProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
    4:string property
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  // system management methods
  void setManagerGoalState(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:ManagerGoalState state
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void shutdown(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:bool stopTabletServers
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void shutdownTabletServer(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tabletServer
    4:bool force
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )
  
  void tabletServerStopping(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tabletServer
    4:string resourceGroup
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void setSystemProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string property
    4:string value
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
    3:ThriftPropertyException tpe
  )
 
  void modifySystemProperties(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:client.TVersionedProperties vProperties
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
    3:client.ThriftConcurrentModificationException tcme
    4:ThriftPropertyException tpe
  )

  void removeSystemProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string property
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void createResourceGroupNode(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string resourceGroup
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )
  
  void removeResourceGroupNode(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string resourceGroup
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
    3:client.ThriftResourceGroupNotExistsException rgne    
  )
    
  void setResourceGroupProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string resourceGroup
    4:string property
    5:string value
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
    3:ThriftPropertyException tpe
    4:client.ThriftResourceGroupNotExistsException rgne
  )
 
  void modifyResourceGroupProperties(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string resourceGroup
    4:client.TVersionedProperties vProperties
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
    3:client.ThriftConcurrentModificationException tcme
    4:ThriftPropertyException tpe
    5:client.ThriftResourceGroupNotExistsException rgne
  )

  void removeResourceGroupProperty(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string resourceGroup
    4:string property
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
    3:client.ThriftResourceGroupNotExistsException rgne
  )


  // system monitoring methods
  ManagerMonitorInfo getManagerStats(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void waitForBalance(
    1:client.TInfo tinfo
  ) throws (
    1:client.ThriftNotActiveServiceException tnase
  )

  oneway void reportTabletStatus(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string serverName
    4:TabletLoadState status
    5:data.TKeyExtent tablet
  )

  list<string> getActiveTservers(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  // Delegation token request
  security.TDelegationToken getDelegationToken(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:security.TDelegationTokenConfig cfg
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void requestTabletHosting(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableId
    4:list<data.TKeyExtent> extents
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException toe
  )

  list<data.TKeyExtent> updateTabletMergeability(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:map<data.TKeyExtent,TTabletMergeability> splits
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException toe
  )

  i64 getManagerTimeNanos(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )
}
