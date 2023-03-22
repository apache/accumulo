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
namespace java org.apache.accumulo.core.tablet.thrift
namespace cpp org.apache.accumulo.core.tablet.thrift

include "data.thrift"
include "security.thrift"
include "client.thrift"
include "tabletserver.thrift"

enum TUnloadTabletGoal {
  UNKNOWN
  UNASSIGNED
  SUSPENDED
  DELETED
}

service TabletManagementClientService {

  oneway void loadTablet(
    5:client.TInfo tinfo
    1:security.TCredentials credentials
    4:string lock
    2:data.TKeyExtent extent
  )

  oneway void unloadTablet(
    5:client.TInfo tinfo
    1:security.TCredentials credentials
    4:string lock
    2:data.TKeyExtent extent
    6:TUnloadTabletGoal goal
    7:i64 requestTime
  )

  void splitTablet(
    4:client.TInfo tinfo
    1:security.TCredentials credentials
    2:data.TKeyExtent extent
    3:binary splitPoint
  ) throws (
    1:client.ThriftSecurityException sec
    2:tabletserver.NotServingTabletException nste
  )

  oneway void flushTablet(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string lock
    4:data.TKeyExtent extent
  )

  oneway void chop(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string lock
    4:data.TKeyExtent extent
  )
  
  void bringOnDemandTabletsOnline(
    1:client.TInfo tinfo
    2:security.TCredentials credentials
    3:string tableId
    4:list<data.TKeyExtent> extents
  ) throws (
    1:client.ThriftSecurityException sec
  )

}
