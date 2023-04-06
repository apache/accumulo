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
package org.apache.accumulo.core.compaction;

import java.util.Map;

public enum CompactionSettings {

  SF_NO_SUMMARY(new NullType(), true),
  SF_EXTRA_SUMMARY(new NullType(), true),
  SF_NO_SAMPLE(new NullType(), true),
  SF_GT_ESIZE_OPT(new SizeType(), true),
  SF_LT_ESIZE_OPT(new SizeType(), true),
  SF_NAME_RE_OPT(new PatternType(), true),
  SF_PATH_RE_OPT(new PatternType(), true),
  MIN_FILES_OPT(new UIntType(), true),
  OUTPUT_COMPRESSION_OPT(new StringType(), false),
  OUTPUT_BLOCK_SIZE_OPT(new SizeType(), false),
  OUTPUT_HDFS_BLOCK_SIZE_OPT(new SizeType(), false),
  OUTPUT_INDEX_BLOCK_SIZE_OPT(new SizeType(), false),
  OUTPUT_REPLICATION_OPT(new UIntType(), false);

  private Type type;
  private boolean selectorOpt;

  private CompactionSettings(Type type, boolean selectorOpt) {
    this.type = type;
    this.selectorOpt = selectorOpt;
  }

  public void put(Map<String,String> selectorOpts, Map<String,String> configurerOpts, String val) {
    if (selectorOpt) {
      selectorOpts.put(name(), type.convert(val));
    } else {
      configurerOpts.put(name(), type.convert(val));
    }
  }
}
