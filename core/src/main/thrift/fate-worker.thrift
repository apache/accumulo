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
namespace java org.apache.accumulo.core.fate.thrift
namespace cpp org.apache.accumulo.core.fate.thrift

include "client.thrift"
include "security.thrift"

struct TFatePartition {
  1:string start
  2:string stop
}

service FateWorkerService {

  list<TFatePartition> getPartitions(
    1:client.TInfo tinfo,
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )

  bool setPartitions(
    1:client.TInfo tinfo,
    2:security.TCredentials credentials,
    3:list<TFatePartition> expected,
    4:list<TFatePartition> desired
   ) throws (
     1:client.ThriftSecurityException sec
   )
}