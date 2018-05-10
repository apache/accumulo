#! /usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script will regenerate the thrift code for accumulo-trace.
INCLUDED_MODULES=(-)
BASE_OUTPUT_PACKAGE='org.apache.accumulo'
PACKAGES_TO_GENERATE=(test.rpc)

. ../core/src/main/scripts/generate-thrift.sh

# fix SimpleThriftService compilation error in Eclipse Oxygen
# see https://bugs.eclipse.org/bugs/show_bug.cgi?id=534559
patch -p2 <<'EOF'
diff --git a/test/src/main/java/org/apache/accumulo/test/rpc/thrift/SimpleThriftService.java b/test/src/main/java/org/apache/accumulo/test/rpc/thrift/SimpleThriftService.java
index d76a789e0..fa0ea29e6 100644
--- a/test/src/main/java/org/apache/accumulo/test/rpc/thrift/SimpleThriftService.java
+++ b/test/src/main/java/org/apache/accumulo/test/rpc/thrift/SimpleThriftService.java
@@ -405,7 +405,7 @@ public class SimpleThriftService {
       super(iface, getProcessMap(processMap));
     }
 
-    private static <I extends Iface> java.util.Map<java.lang.String,  org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>> getProcessMap(java.util.Map<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
+    private static <I extends Iface> java.util.Map<java.lang.String,  org.apache.thrift.ProcessFunction<I, ?>> getProcessMap(java.util.Map<java.lang.String, org.apache.thrift.ProcessFunction<I, ? extends  org.apache.thrift.TBase>> processMap) {
       processMap.put("echoPass", new echoPass());
       processMap.put("onewayPass", new onewayPass());
       processMap.put("echoFail", new echoFail());
EOF
