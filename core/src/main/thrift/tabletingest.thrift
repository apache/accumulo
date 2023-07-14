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
namespace java org.apache.accumulo.core.tabletingest.thrift
namespace cpp org.apache.accumulo.core.tabletingest.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "manager.thrift"
include "tabletserver.thrift"

exception ConstraintViolationException {
  1:list<data.TConstraintViolationSummary> violationSummaries
}

enum TDurability {
  DEFAULT = 0
  SYNC = 1
  FLUSH = 2
  LOG = 3
  NONE = 4
}

struct DataFileInfo {
  1:i64 estimatedSize
}

service TabletIngestClientService {

  //the following calls support a batch update to multiple tablets on a tablet server
  data.UpdateID startUpdate(
    2:client.TInfo tinfo
    1:security.TCredentials credentials
    3:TDurability durability
  ) throws (
    1:client.ThriftSecurityException sec
  )

  oneway void applyUpdates(
    1:client.TInfo tinfo
    2:data.UpdateID updateID
    3:data.TKeyExtent keyExtent
    4:list<data.TMutation> mutations
  )

  data.UpdateErrors closeUpdate(
    2:client.TInfo tinfo
    1:data.UpdateID updateID
  ) throws (
    1:tabletserver.NoSuchScanIDException nssi
  )

  //the following call supports making a single update to a tablet
  void update(
    4:client.TInfo tinfo
    1:security.TCredentials credentials
    2:data.TKeyExtent keyExtent
    3:data.TMutation mutation
    5:TDurability durability
  ) throws (
    1:client.ThriftSecurityException sec
    2:tabletserver.NotServingTabletException nste
    3:ConstraintViolationException cve
  )

  data.TConditionalSession startConditionalUpdate(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:list<binary> authorizations
    4:string tableID
    5:TDurability durability
    6:string classLoaderContext
  ) throws (
    1:client.ThriftSecurityException sec
  )

  list<data.TCMResult> conditionalUpdate(
    1:client.TInfo tinfo
    2:data.UpdateID sessID
    3:data.CMBatch mutations
    4:list<string> symbols
  ) throws (
    1:tabletserver.NoSuchScanIDException nssi
  )

  void invalidateConditionalUpdate(
    1:client.TInfo tinfo
    2:data.UpdateID sessID
  )

  oneway void closeConditionalUpdate(
    1:client.TInfo tinfo
    2:data.UpdateID sessID
  )
}
