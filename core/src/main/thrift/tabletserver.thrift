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
namespace java org.apache.accumulo.core.tabletserver.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "master.thrift"
include "cloudtrace.thrift"

exception NotServingTabletException {
  1:data.TKeyExtent extent
}

exception TooManyFilesException {
  1:data.TKeyExtent extent
}

exception NoSuchScanIDException {
}

exception ConstraintViolationException {
  1:list<data.TConstraintViolationSummary> violationSummaries
}

struct ActionStats {
    1:i32 status
    2:double elapsed
    3:i32 num
    4:i64 count
    5:double sumDev  // sum of the square of the elapsed time
    6:i32 fail
    7:double queueTime
    8:double queueSumDev // sum of the square of the queue time
}

struct TabletStats {
    1:data.TKeyExtent extent
    2:ActionStats major
    3:ActionStats minor
    4:ActionStats split
    5:i64 numEntries
    6:double ingestRate
    7:double queryRate
    // zero if loaded by the master, currentTimeMillis when the split was created
    8:i64 splitCreationTime
}

enum ScanType {
    SINGLE,
    BATCH
}

enum ScanState {
    IDLE,
    RUNNING,
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
    12:map<string, map<string, string>> ssio  /* Server Side Iterator Options */
}

struct TIteratorSetting {
    1:i32 priority;
    2:string name;
    3:string iteratorClass;
    4:map<string,string> properties;
}

struct IteratorConfig {
   1:list<TIteratorSetting> iterators;
}

service TabletClientService extends client.ClientService {
  // scan a range of keys
  data.InitialScan startScan(11:cloudtrace.TInfo tinfo,
                             1:security.AuthInfo credentials,
                             2:data.TKeyExtent extent,
                             3:data.TRange range,
                             4:list<data.TColumn> columns,
                             5:i32 batchSize,
                             6:list<data.IterInfo> ssiList,
                             7:map<string, map<string, string>> ssio,
                             8:list<binary> authorizations
                             9:bool waitForWrites,
                             10:bool isolated)  throws (1:security.ThriftSecurityException sec, 2:NotServingTabletException nste, 3:TooManyFilesException tmfe),
                             
  data.ScanResult continueScan(2:cloudtrace.TInfo tinfo, 1:data.ScanID scanID)  throws (1:NoSuchScanIDException nssi, 2:NotServingTabletException nste, 3:TooManyFilesException tmfe),
  oneway void closeScan(2:cloudtrace.TInfo tinfo, 1:data.ScanID scanID),

  // scan over a series of ranges
  data.InitialMultiScan startMultiScan(8:cloudtrace.TInfo tinfo,
                                  1:security.AuthInfo credentials,
                                  2:data.ScanBatch batch,
                                  3:list<data.TColumn> columns,
                                  4:list<data.IterInfo> ssiList,
                                  5:map<string, map<string, string>> ssio,
                                  6:list<binary> authorizations
                                  7:bool waitForWrites)  throws (1:security.ThriftSecurityException sec),
  data.MultiScanResult continueMultiScan(2:cloudtrace.TInfo tinfo, 1:data.ScanID scanID) throws (1:NoSuchScanIDException nssi),
  void closeMultiScan(2:cloudtrace.TInfo tinfo, 1:data.ScanID scanID) throws (1:NoSuchScanIDException nssi),
  
  //the following calls support a batch update to multiple tablets on a tablet server
  data.UpdateID startUpdate(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec),
  oneway void applyUpdates(1:cloudtrace.TInfo tinfo, 2:data.UpdateID updateID, 3:data.TKeyExtent keyExtent, 4:list<data.TMutation> mutations),
  data.UpdateErrors closeUpdate(2:cloudtrace.TInfo tinfo, 1:data.UpdateID updateID) throws (1:NoSuchScanIDException nssi),
  
  //the following call supports making a single update to a tablet
  void update(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:data.TKeyExtent keyExtent, 3:data.TMutation mutation)
    throws (1:security.ThriftSecurityException sec, 
            2:NotServingTabletException nste, 
            3:ConstraintViolationException cve),
  
  // on success, returns an empty list
  list<data.TKeyExtent> bulkImport(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 4:i64 tid, 2:data.TabletFiles files, 5:bool setTime) throws (1:security.ThriftSecurityException sec),

  void splitTablet(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:data.TKeyExtent extent, 3:binary splitPoint) throws (1:security.ThriftSecurityException sec, 2:NotServingTabletException nste)
 
  oneway void loadTablet(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 4:string lock, 2:data.TKeyExtent extent),
  oneway void unloadTablet(5:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 4:string lock, 2:data.TKeyExtent extent, 3:bool save),
  oneway void flush(4:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 3:string lock, 2:string tableId, 5:binary startRow, 6:binary endRow),
  oneway void flushTablet(1:cloudtrace.TInfo tinfo, 2:security.AuthInfo credentials, 3:string lock, 4:data.TKeyExtent extent),
  oneway void chop(1:cloudtrace.TInfo tinfo, 2:security.AuthInfo credentials, 3:string lock, 4:data.TKeyExtent extent),
  oneway void compact(1:cloudtrace.TInfo tinfo, 2:security.AuthInfo credentials, 3:string lock, 4:string tableId, 5:binary startRow, 6:binary endRow),
  
  master.TabletServerStatus getTabletServerStatus(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)
  list<TabletStats> getTabletStats(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string tableId) throws (1:security.ThriftSecurityException sec)
  TabletStats getHistoricalStats(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)
  void halt(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string lock) throws (1:security.ThriftSecurityException sec)
  oneway void fastHalt(3:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials, 2:string lock);
  
  list<ActiveScan> getActiveScans(2:cloudtrace.TInfo tinfo, 1:security.AuthInfo credentials) throws (1:security.ThriftSecurityException sec)
  oneway void removeLogs(1:cloudtrace.TInfo tinfo, 2:security.AuthInfo credentials, 3:list<string> filenames)
}

typedef i32 TabletID

struct TabletMutations {
	1:TabletID tabletID,
	2:i64 seq,
	3:list<data.TMutation> mutations
}
