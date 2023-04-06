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
namespace java org.apache.accumulo.core.dataImpl.thrift
namespace cpp org.apache.accumulo.core.dataImpl.thrift

include "security.thrift"
include "client.thrift"

typedef i64 ScanID
typedef i64 UpdateID

struct TKey {
  1:binary row
  2:binary colFamily
  3:binary colQualifier
  4:binary colVisibility
  5:i64 timestamp
}

struct TColumn {
  1:binary columnFamily
  2:binary columnQualifier
  3:binary columnVisibility
}

struct TMutation {
  1:binary row
  2:binary data
  3:list<binary> values
  4:i32 entries
  5:optional list<string> sources
}

struct TKeyExtent {
  1:binary table
  2:binary endRow
  3:binary prevEndRow
}

struct TKeyValue {
  1:TKey key
  2:binary value
}

struct ScanResult {
  1:list<TKeyValue> results
  2:bool more
}

struct TRange {
  1:TKey start
  2:TKey stop
  3:bool startKeyInclusive
  4:bool stopKeyInclusive
  5:bool infiniteStartKey
  6:bool infiniteStopKey
}

typedef map<TKeyExtent, list<TRange>> ScanBatch

struct MultiScanResult {
  1:list<TKeyValue> results
  2:ScanBatch failures
  3:list<TKeyExtent> fullScans
  4:TKeyExtent partScan
  5:TKey partNextKey
  6:bool partNextKeyInclusive
  7:bool more
}

struct InitialScan {
  1:ScanID scanID
  2:ScanResult result
}

struct InitialMultiScan {
  1:ScanID scanID
  2:MultiScanResult result
}

struct IterInfo {
  1:i32 priority
  2:string className
  3:string iterName
}

struct TConstraintViolationSummary {
  1:string constrainClass
  2:i16 violationCode
  3:string violationDescription
  4:i64 numberOfViolatingMutations
}

struct UpdateErrors {
  1:map<TKeyExtent, i64> failedExtents
  2:list<TConstraintViolationSummary> violationSummaries
  3:map<TKeyExtent, client.SecurityErrorCode> authorizationFailures
}

enum TCMStatus {
  ACCEPTED
  REJECTED
  VIOLATED
  IGNORED
}

struct TCMResult {
  1:i64 cmid
  2:TCMStatus status
}

struct MapFileInfo {
  1:i64 estimatedSize
}

struct TCondition {
  1:binary cf
  2:binary cq
  3:binary cv
  4:i64 ts
  5:bool hasTimestamp
  6:binary val
  7:binary iterators
}

struct TConditionalMutation {
  1:list<TCondition> conditions
  2:TMutation mutation
  3:i64 id
}

struct TConditionalSession {
  1:i64 sessionId
  2:string tserverLock
  3:i64 ttl
}

struct TSummarizerConfiguration {
  1:string classname
  2:map<string, string> options
  3:string configId
}

struct TSummary {
  1:map<string, i64> summary
  2:TSummarizerConfiguration config
  3:i64 filesContaining
  4:i64 filesExceeding
  5:i64 filesLarge
}

struct TSummaries {
  1:bool finished
  2:i64 sessionId
  3:i64 totalFiles
  4:i64 deletedFiles
  5:list<TSummary> summaries
}

struct TRowRange {
  1:binary startRow
  2:binary endRow
}

struct TSummaryRequest {
  1:string tableId
  2:TRowRange bounds
  3:list<TSummarizerConfiguration> summarizers
  4:string summarizerPattern
}

typedef map<TKeyExtent, list<TConditionalMutation>> CMBatch

typedef map<TKeyExtent, list<TMutation>> UpdateBatch

typedef map<TKeyExtent, map<string, MapFileInfo>> TabletFiles

