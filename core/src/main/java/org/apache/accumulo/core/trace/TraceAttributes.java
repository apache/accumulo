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
package org.apache.accumulo.core.trace;

import io.opentelemetry.api.common.AttributeKey;

public class TraceAttributes {
  public static final AttributeKey<Long> ENTRIES_RETURNED_KEY =
      AttributeKey.longKey("accumulo.scan.entries.returned");
  public static final AttributeKey<Long> BYTES_RETURNED_KEY =
      AttributeKey.longKey("accumulo.scan.bytes.returned");
  public static final AttributeKey<Long> BYTES_READ_KEY =
      AttributeKey.longKey("accumulo.scan.bytes.read");
  public static final AttributeKey<Long> BYTES_READ_FILE_KEY =
      AttributeKey.longKey("accumulo.scan.bytes.read.file");
  public static final AttributeKey<String> EXECUTOR_KEY =
      AttributeKey.stringKey("accumulo.scan.executor");
  public static final AttributeKey<String> TABLE_ID_KEY =
      AttributeKey.stringKey("accumulo.table.id");
  public static final AttributeKey<String> EXTENT_KEY = AttributeKey.stringKey("accumulo.extent");
  public static final AttributeKey<Long> INDEX_HITS_KEY =
      AttributeKey.longKey("accumulo.scan.cache.index.hits");
  public static final AttributeKey<Long> INDEX_MISSES_KEY =
      AttributeKey.longKey("accumulo.scan.cache.index.misses");
  public static final AttributeKey<Long> INDEX_BYPASSES_KEY =
      AttributeKey.longKey("accumulo.scan.cache.index.bypasses");
  public static final AttributeKey<Long> DATA_HITS_KEY =
      AttributeKey.longKey("accumulo.scan.cache.data.hits");
  public static final AttributeKey<Long> DATA_MISSES_KEY =
      AttributeKey.longKey("accumulo.scan.cache.data.misses");
  public static final AttributeKey<Long> DATA_BYPASSES_KEY =
      AttributeKey.longKey("accumulo.scan.cache.data.bypasses");
  public static final AttributeKey<String> SERVER_KEY = AttributeKey.stringKey("accumulo.server");
  public static final AttributeKey<Long> ENTRIES_READ_KEY =
      AttributeKey.longKey("accumulo.scan.entries.read");
  public static final AttributeKey<Long> SEEKS_KEY = AttributeKey.longKey("accumulo.scan.seeks");
}
