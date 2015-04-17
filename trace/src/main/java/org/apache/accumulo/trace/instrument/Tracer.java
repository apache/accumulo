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
package org.apache.accumulo.trace.instrument;

/**
 * ACCUMULO-3738: Temporary fix to keep Hive working with all versions of Accumulo without extra burden on users. Hive referenced this class in the build-up of
 * the classpath used to compute the answer to a query. Without this class, the accumulo-trace jar would not make it onto the classpath which would break the
 * query. Whenever Hive can get this patch into their build and a sufficient number of releases pass, we can remove this class.
 *
 * Accumulo should not reference this class at all. It is solely here for Hive integration.
 */
@Deprecated
public class Tracer {

}
