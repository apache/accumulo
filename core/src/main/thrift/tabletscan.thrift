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
namespace java org.apache.accumulo.core.tabletscan.thrift
namespace cpp org.apache.accumulo.core.tabletscan.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "manager.thrift"
include "master.thrift"
include "tabletserver.thrift"

exception TooManyFilesException {
  1:data.TKeyExtent extent
}

exception TSampleNotPresentException {
  1:data.TKeyExtent extent
}

exception ScanServerBusyException {}

struct TSamplerConfiguration {
  1:string className
  2:map<string, string> options
}

enum ScanType {
  SINGLE
  BATCH
}

enum ScanState {
  IDLE
  RUNNING
  QUEUED
}

struct ActiveScan {
  2:string client
  3:string user
  4:string tableId
  5:i64 age
  6:i64 idleTime
  7:ScanType type
  8:ScanState state
  9:data.TKeyExtent extent
  10:list<data.TColumn> columns
  11:list<data.IterInfo> ssiList
  // Server Side Iterator Options
  12:map<string, map<string, string>> ssio
  13:list<binary> authorizations
  14:optional i64 scanId
  // name of the classloader context
  15:string classLoaderContext
  16:string userData
}


service TabletScanClientService {

  // scan a range of keys
  data.InitialScan startScan(
    11:client.TInfo tinfo
    1:security.TCredentials credentials
    2:data.TKeyExtent extent
    3:data.TRange range
    4:list<data.TColumn> columns
    5:i32 batchSize
    6:list<data.IterInfo> ssiList
    7:map<string, map<string, string>> ssio
    8:list<binary> authorizations
    9:bool waitForWrites
    10:bool isolated
    12:i64 readaheadThreshold
    13:TSamplerConfiguration samplerConfig
    14:i64 batchTimeOut
    // name of the classloader context
    15:string classLoaderContext
    16:map<string, string> executionHints
    17:i64 busyTimeout
    18:string userData
  ) throws (
    1:client.ThriftSecurityException sec
    2:tabletserver.NotServingTabletException nste
    3:TooManyFilesException tmfe
    4:TSampleNotPresentException tsnpe
    5:ScanServerBusyException ssbe
  )

  data.ScanResult continueScan(
    2:client.TInfo tinfo
    1:data.ScanID scanID
    3:i64 busyTimeout
  ) throws (
    1:tabletserver.NoSuchScanIDException nssi
    2:tabletserver.NotServingTabletException nste
    3:TooManyFilesException tmfe
    4:TSampleNotPresentException tsnpe
    5:ScanServerBusyException ssbe
  )

  oneway void closeScan(
    2:client.TInfo tinfo
    1:data.ScanID scanID
  )

  // scan over a series of ranges
  data.InitialMultiScan startMultiScan(
    8:client.TInfo tinfo
    1:security.TCredentials credentials
    2:data.ScanBatch batch
    3:list<data.TColumn> columns
    4:list<data.IterInfo> ssiList
    5:map<string, map<string, string>> ssio
    6:list<binary> authorizations
    7:bool waitForWrites
    9:TSamplerConfiguration samplerConfig
    10:i64 batchTimeOut
    // name of the classloader context
    11:string classLoaderContext
    12:map<string, string> executionHints
    13:i64 busyTimeout
    14:string userData
  ) throws (
    1:client.ThriftSecurityException sec
    2:TSampleNotPresentException tsnpe
    3:ScanServerBusyException ssbe
  )

  data.MultiScanResult continueMultiScan(
    2:client.TInfo tinfo
    1:data.ScanID scanID
    3:i64 busyTimeout
  ) throws (
    1:tabletserver.NoSuchScanIDException nssi
    2:TSampleNotPresentException tsnpe
    3:ScanServerBusyException ssbe
  )

  void closeMultiScan(
    2:client.TInfo tinfo
    1:data.ScanID scanID
  ) throws (
    1:tabletserver.NoSuchScanIDException nssi
  )

  list<ActiveScan> getActiveScans(
    2:client.TInfo tinfo
    1:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

}
