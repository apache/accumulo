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
namespace java org.apache.accumulo.core.process.thrift
namespace cpp org.apache.accumulo.core.process.thrift

include "client.thrift"
include "security.thrift"

enum MetricSource {
  COMPACTOR
  GARBAGE_COLLECTOR
  MANAGER
  SCAN_SERVER
  TABLET_SERVER
}

struct MetricResponse {
  1:MetricSource serverType
  2:string server
  3:string resourceGroup
  4:i64 timestamp
  5:list<binary> metrics
}

service ServerProcessService {

  MetricResponse getMetrics(
    2:security.TCredentials credentials  
  ) throws (
    1:client.ThriftSecurityException sec
  )

  oneway void gracefulShutdown(
    1:security.TCredentials credentials  
  )

}
 