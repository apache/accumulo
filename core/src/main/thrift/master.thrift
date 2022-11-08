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
  9:Compacting minors
  10:Compacting majors
  11:Compacting scans
  12:double scanRate
}

struct RecoveryStatus {
  2:string name
  // in millis
  5:i32 runtime
  6:double progress
}

enum BulkImportState {
  INITIAL
  // manager moves the files into the accumulo area
  MOVING
  // tserver examines the index of the file
  PROCESSING
  // tserver assigns the file to tablets
  ASSIGNING
  // tserver incorporates file into tablet
  LOADING
  // manager moves error files into the error directory
  COPY_FILES
  // flags and locks removed
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
  19:string version
  18:i64 responseTime
}
