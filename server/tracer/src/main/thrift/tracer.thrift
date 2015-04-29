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
namespace java org.apache.accumulo.tracer.thrift
namespace cpp org.apache.accumulo.tracer.thrift

include "trace.thrift"

struct Annotation {
   1:i64 time,
   2:string msg
}

struct RemoteSpan {
   1:string sender,
   2:string svc, 
   3:i64 traceId, 
   4:i64 spanId, 
   5:i64 parentId, 
   6:i64 start, 
   7:i64 stop, 
   8:string description, 
   9:map<string, string> data,
   10:list<Annotation> annotations
}

service SpanReceiver {
   oneway void span(1:RemoteSpan span);
}

// used for testing trace
service TestService {
   bool checkTrace(1:trace.TInfo tinfo, 2:string message),
}
