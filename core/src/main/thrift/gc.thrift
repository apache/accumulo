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
namespace java org.apache.accumulo.core.gc.thrift
namespace cpp org.apache.accumulo.core.gc.thrift

include "security.thrift"
include "trace.thrift"
include "client.thrift"

struct GcCycleStats {
   1:i64 started;
   2:i64 finished;
   3:i64 candidates;
   4:i64 inUse;
   5:i64 deleted;
   6:i64 errors;
}

struct GCStatus {
   1:GcCycleStats last;
   2:GcCycleStats lastLog;
   3:GcCycleStats current;
   4:GcCycleStats currentLog;
}


service GCMonitorService {
   GCStatus getStatus(2:trace.TInfo tinfo, 1:security.TCredentials credentials) throws (1:client.ThriftSecurityException sec);
}
