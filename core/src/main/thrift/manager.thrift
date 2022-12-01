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
include "trace.thrift"
include "master.thrift"

struct DeadServer {
  1:string server
  2:i64 lastStatus
  3:string status
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
  TABLE_BULK_IMPORT2
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

struct ManagerMonitorInfo {
  1:map<string, master.TableInfo> tableMap
  2:list<master.TabletServerStatus> tServerInfo
  3:map<string, i8> badTServers
  4:ManagerState state
  5:ManagerGoalState goalState
  6:i32 unassignedTablets
  7:set<string> serversShuttingDown
  8:list<DeadServer> deadTabletServers
  9:list<master.BulkImportStatus> bulkImports
}

service FateService {

  // register a fate operation by reserving an opid
  i64 beginFateOperation(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  // initiate execution of the fate operation; set autoClean to true if not waiting for completion
  void executeFateOperation(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:i64 opid
    4:FateOperation op
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
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:i64 opid
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  // clean up fate operation if autoClean was not set, after waiting
  void finishFateOperation(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:i64 opid
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  // cancel a fate operation
  bool cancelFateOperation(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:i64 opid
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )
  
}

service ManagerClientService {

  // table management methods
  i64 initiateFlush(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  void waitForFlush(
    1:trace.TInfo tinfo
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
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:string property
    5:string value
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  void modifyTableProperties(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:client.TVersionedProperties vProperties
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
    4:client.ThriftConcurrentModificationException tcme
  )

  void removeTableProperty(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableName
    4:string property
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  void setNamespaceProperty(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
    4:string property
    5:string value
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
  )

  void modifyNamespaceProperties(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string ns
    4:client.TVersionedProperties vProperties
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
    3:client.ThriftNotActiveServiceException tnase
    4:client.ThriftConcurrentModificationException tcme
  )

  void removeNamespaceProperty(
    1:trace.TInfo tinfo
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
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:ManagerGoalState state
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void shutdown(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:bool stopTabletServers
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void shutdownTabletServer(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string tabletServer
    4:bool force
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void setSystemProperty(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string property
    4:string value
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )
 
  void modifySystemProperties(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:client.TVersionedProperties vProperties
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
    3:client.ThriftConcurrentModificationException tcme
  )

  void removeSystemProperty(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string property
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  // system monitoring methods
  ManagerMonitorInfo getManagerStats(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  void waitForBalance(
    1:trace.TInfo tinfo
  ) throws (
    1:client.ThriftNotActiveServiceException tnase
  )

  // tablet server reporting
  oneway void reportSplitExtent(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string serverName
    4:TabletSplit split
  )

  oneway void reportTabletStatus(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:string serverName
    4:TabletLoadState status
    5:data.TKeyExtent tablet
  )

  list<string> getActiveTservers(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

  // Delegation token request
  security.TDelegationToken getDelegationToken(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
    3:security.TDelegationTokenConfig cfg
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftNotActiveServiceException tnase
  )

}
