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
namespace java org.apache.accumulo.core.trace.thrift
namespace cpp org.apache.accumulo.core.trace.thrift

# OpenTelemetry uses the standards at https://www.w3.org/TR/trace-context/
# to propagate information across process boundaries.
struct TInfo {
  // 1:i64 traceId - removed in 2.1.0
  // 2:i64 parentId - removed in 2.1.0  
  3:map<string,string> headers
}
