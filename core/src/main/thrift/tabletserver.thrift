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
namespace java org.apache.accumulo.core.tabletserver.thrift
namespace cpp org.apache.accumulo.core.tabletserver.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "manager.thrift"

exception NotServingTabletException {
  1:data.TKeyExtent extent
}

exception NoSuchScanIDException {}

struct ActionStats {
  1:i32 status
  2:double elapsed
  3:i32 num
  4:i64 count
  // sum of the square of the elapsed time
  5:double sumDev
  6:i32 fail
  7:double queueTime
  // sum of the square of the queue time
  8:double queueSumDev
}

struct TabletStats {
  1:data.TKeyExtent extent
  // ELASTICITY_TODO comment out following field and stop reading it, its not being populated anymore
  2:ActionStats majors
  3:ActionStats minors
  4:ActionStats splits
  5:i64 numEntries
  6:double ingestRate
  7:double queryRate
  // do not reuse field 8, it was dropped
  //8:i64 splitCreationTime
}

enum TCompactionType {
  MINOR
  MERGE
  MAJOR
  FULL
}

enum TCompactionReason {
  USER
  SYSTEM
  IDLE
  CLOSE
}

struct ActiveCompaction {
  1:data.TKeyExtent extent
  2:i64 age
  3:list<string> inputFiles
  4:string outputFile
  5:TCompactionType type
  6:TCompactionReason reason
  7:string localityGroup
  8:i64 entriesRead
  9:i64 entriesWritten
  10:list<data.IterInfo> ssiList
  11:map<string, map<string, string>> ssio
  12:i64 timesPaused
}

struct TIteratorSetting {
  1:i32 priority
  2:string name
  3:string iteratorClass
  4:map<string, string> properties
}

struct IteratorConfig {
  1:list<TIteratorSetting> iterators
}

struct InputFile {
  1:string metadataFileEntry
  2:i64 size
  3:i64 entries
  4:i64 timestamp
}

struct TExternalCompactionJob {
  1:string externalCompactionId
  2:data.TKeyExtent extent
  3:list<InputFile> files
  4:IteratorConfig iteratorSettings
  5:string outputFile
  6:bool propagateDeletes
  7:TCompactionKind kind
  8:manager.TFateId fateId
  9:map<string, string> overrides
}

enum TCompactionKind {
  SELECTOR
  SYSTEM
  USER
}

struct TCompactionGroupSummary {
  1:string group
  2:i16 priority
}

struct TCompactionStats{
  1:i64 entriesRead;
  2:i64 entriesWritten;
  3:i64 fileSize;
}

service TabletServerClientService {

  oneway void flush(
    4:client.TInfo tinfo
    1:security.TCredentials credentials
    3:string lock
    2:string tableId
    5:binary startRow
    6:binary endRow
  )

  manager.TabletServerStatus getTabletServerStatus(
    3:client.TInfo tinfo
    1:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  list<TabletStats> getTabletStats(
    3:client.TInfo tinfo
    1:security.TCredentials credentials
    2:string tableId
  ) throws (
    1:client.ThriftSecurityException sec
  )

  TabletStats getHistoricalStats(
    2:client.TInfo tinfo
    1:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  void halt(
    3:client.TInfo tinfo
    1:security.TCredentials credentials
    2:string lock
  ) throws (
    1:client.ThriftSecurityException sec
  )

  oneway void fastHalt(
    3:client.TInfo tinfo
    1:security.TCredentials credentials
    2:string lock
  )

  list<ActiveCompaction> getActiveCompactions(
    2:client.TInfo tinfo
    1:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  oneway void removeLogs(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:list<string> filenames
  )

  list<string> getActiveLogs(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
  )

  data.TSummaries startGetSummaries(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:data.TSummaryRequest request
  ) throws (
    1:client.ThriftSecurityException sec
    2:client.ThriftTableOperationException tope
  )

  data.TSummaries startGetSummariesForPartition(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:data.TSummaryRequest request
    4:i32 modulus
    5:i32 remainder
  ) throws (
    1:client.ThriftSecurityException sec
  )

  data.TSummaries startGetSummariesFromFiles(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:data.TSummaryRequest request
    4:map<string, list<data.TRowRange>> files
  ) throws (
    1:client.ThriftSecurityException sec
  )

  data.TSummaries contiuneGetSummaries(
    1:client.TInfo tinfo
    2:i64 sessionId
  ) throws (
    1:NoSuchScanIDException nssi
  )
  
  list<data.TKeyExtent> refreshTablets(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:list<data.TKeyExtent> tabletsToRefresh
  )

  map<data.TKeyExtent, i64> allocateTimestamps(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:list<data.TKeyExtent> tablets
    4:i32 numStamps
  )
}

typedef i32 TabletID
