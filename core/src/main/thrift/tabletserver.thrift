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
namespace cpp org.apache.accumulo.core.tabletserver.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "master.thrift"
include "trace.thrift"

exception NotServingTabletException {
  1:data.TKeyExtent extent
}

exception TooManyFilesException {
  1:data.TKeyExtent extent
}

exception TSampleNotPresentException {
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
    2:ActionStats majors
    3:ActionStats minors
    4:ActionStats splits
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
    13:list<binary> authorizations
    14:optional i64 scanId
    15:string classLoaderContext /* name of the classloader context */
}

enum CompactionType {
   MINOR,
   MERGE,
   MAJOR,
   FULL
}

enum CompactionReason {
   USER,
   SYSTEM,
   CHOP,
   IDLE,
   CLOSE
}

enum TDurability {
  DEFAULT = 0,
  SYNC = 1,
  FLUSH = 2,
  LOG = 3,
  NONE = 4
}

struct ActiveCompaction {
    1:data.TKeyExtent extent
    2:i64 age
    3:list<string> inputFiles
    4:string outputFile
    5:CompactionType type
    6:CompactionReason reason
    7:string localityGroup
    8:i64 entriesRead
    9:i64 entriesWritten
    10:list<data.IterInfo> ssiList
    11:map<string, map<string, string>> ssio 
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

struct TSamplerConfiguration {
   1:string className
   2:map<string, string> options
}

enum TUnloadTabletGoal {
   UNKNOWN,
   UNASSIGNED,
   SUSPENDED,
   DELETED
}

service TabletClientService extends client.ClientService {
  // scan a range of keys
  data.InitialScan startScan(11:trace.TInfo tinfo,
                             1:security.TCredentials credentials,
                             2:data.TKeyExtent extent,
                             3:data.TRange range,
                             4:list<data.TColumn> columns,
                             5:i32 batchSize,
                             6:list<data.IterInfo> ssiList,
                             7:map<string, map<string, string>> ssio,
                             8:list<binary> authorizations
                             9:bool waitForWrites,
                             10:bool isolated,
                             12:i64 readaheadThreshold,
                             13:TSamplerConfiguration samplerConfig,
                             14:i64 batchTimeOut,
                             15:string classLoaderContext /* name of the classloader context */)  throws (1:client.ThriftSecurityException sec, 2:NotServingTabletException nste, 3:TooManyFilesException tmfe, 4:TSampleNotPresentException tsnpe),
                             
  data.ScanResult continueScan(2:trace.TInfo tinfo, 1:data.ScanID scanID)  throws (1:NoSuchScanIDException nssi, 2:NotServingTabletException nste, 3:TooManyFilesException tmfe, 4:TSampleNotPresentException tsnpe),
  oneway void closeScan(2:trace.TInfo tinfo, 1:data.ScanID scanID),

  // scan over a series of ranges
  data.InitialMultiScan startMultiScan(8:trace.TInfo tinfo,
                                  1:security.TCredentials credentials,
                                  2:data.ScanBatch batch,
                                  3:list<data.TColumn> columns,
                                  4:list<data.IterInfo> ssiList,
                                  5:map<string, map<string, string>> ssio,
                                  6:list<binary> authorizations,
                                  7:bool waitForWrites,
                                  9:TSamplerConfiguration samplerConfig,
                                  10:i64 batchTimeOut,
                                  11:string classLoaderContext /* name of the classloader context */)  throws (1:client.ThriftSecurityException sec, 2:TSampleNotPresentException tsnpe),
  data.MultiScanResult continueMultiScan(2:trace.TInfo tinfo, 1:data.ScanID scanID) throws (1:NoSuchScanIDException nssi, 2:TSampleNotPresentException tsnpe),
  void closeMultiScan(2:trace.TInfo tinfo, 1:data.ScanID scanID) throws (1:NoSuchScanIDException nssi),
  
  //the following calls support a batch update to multiple tablets on a tablet server
  data.UpdateID startUpdate(2:trace.TInfo tinfo, 1:security.TCredentials credentials, 3:TDurability durability) throws (1:client.ThriftSecurityException sec),
  oneway void applyUpdates(1:trace.TInfo tinfo, 2:data.UpdateID updateID, 3:data.TKeyExtent keyExtent, 4:list<data.TMutation> mutations),
  data.UpdateErrors closeUpdate(2:trace.TInfo tinfo, 1:data.UpdateID updateID) throws (1:NoSuchScanIDException nssi),

  //the following call supports making a single update to a tablet
  void update(4:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:data.TKeyExtent keyExtent, 3:data.TMutation mutation, 5:TDurability durability)
    throws (1:client.ThriftSecurityException sec, 
            2:NotServingTabletException nste, 
            3:ConstraintViolationException cve),

  data.TConditionalSession startConditionalUpdate(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:list<binary> authorizations, 4:string tableID, 5:TDurability durability, 6:string classLoaderContext)
     throws (1:client.ThriftSecurityException sec);
  
  list<data.TCMResult> conditionalUpdate(1:trace.TInfo tinfo, 2:data.UpdateID sessID, 3:data.CMBatch mutations, 4:list<string> symbols)
     throws (1:NoSuchScanIDException nssi);
  void invalidateConditionalUpdate(1:trace.TInfo tinfo, 2:data.UpdateID sessID);
  oneway void closeConditionalUpdate(1:trace.TInfo tinfo, 2:data.UpdateID sessID);

  // on success, returns an empty list
  list<data.TKeyExtent> bulkImport(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 4:i64 tid, 2:data.TabletFiles files, 5:bool setTime) throws (1:client.ThriftSecurityException sec),

  void splitTablet(4:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:data.TKeyExtent extent, 3:binary splitPoint) throws (1:client.ThriftSecurityException sec, 2:NotServingTabletException nste)
 
  oneway void loadTablet(5:trace.TInfo tinfo, 1:security.TCredentials credentials, 4:string lock, 2:data.TKeyExtent extent),
  oneway void unloadTablet(5:trace.TInfo tinfo, 1:security.TCredentials credentials, 4:string lock, 2:data.TKeyExtent extent, 6:TUnloadTabletGoal goal, 7:i64 requestTime),
  oneway void flush(4:trace.TInfo tinfo, 1:security.TCredentials credentials, 3:string lock, 2:string tableId, 5:binary startRow, 6:binary endRow),
  oneway void flushTablet(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:string lock, 4:data.TKeyExtent extent),
  oneway void chop(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:string lock, 4:data.TKeyExtent extent),
  oneway void compact(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:string lock, 4:string tableId, 5:binary startRow, 6:binary endRow),
  
  master.TabletServerStatus getTabletServerStatus(3:trace.TInfo tinfo, 1:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec)
  list<TabletStats> getTabletStats(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string tableId) throws (1:client.ThriftSecurityException sec)
  TabletStats getHistoricalStats(2:trace.TInfo tinfo, 1:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec)
  void halt(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string lock) throws (1:client.ThriftSecurityException sec)
  oneway void fastHalt(3:trace.TInfo tinfo, 1:security.TCredentials credentials, 2:string lock);
  
  list<ActiveScan> getActiveScans(2:trace.TInfo tinfo, 1:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec)
  list<ActiveCompaction> getActiveCompactions(2:trace.TInfo tinfo, 1:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec)
  oneway void removeLogs(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:list<string> filenames)
  list<string> getActiveLogs(1:trace.TInfo tinfo, 2:security.TCredentials credentials)

  data.TSummaries startGetSummaries(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:data.TSummaryRequest request) throws (1:client.ThriftSecurityException sec, 2:client.ThriftTableOperationException tope)
  data.TSummaries startGetSummariesForPartition(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:data.TSummaryRequest request, 4:i32 modulus, 5:i32 remainder) throws (1:client.ThriftSecurityException sec)
  data.TSummaries startGetSummariesFromFiles(1:trace.TInfo tinfo, 2:security.TCredentials credentials, 3:data.TSummaryRequest request, 4:map<string,list<data.TRowRange>> files) throws (1:client.ThriftSecurityException sec)
  data.TSummaries contiuneGetSummaries(1:trace.TInfo tinfo, 2:i64 sessionId) throws (1:NoSuchScanIDException nssi)
}

typedef i32 TabletID
