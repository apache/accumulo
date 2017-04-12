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

package org.apache.accumulo.core.compaction;

import java.util.Map;

public enum CompactionSettings {

  SF_NO_SUMMARY(new NullType()),
  SF_EXTRA_SUMMARY(new NullType()),
  SF_NO_SAMPLE(new NullType()),
  SF_GT_ESIZE_OPT(new SizeType()),
  SF_LT_ESIZE_OPT(new SizeType()),
  SF_NAME_RE_OPT(new PatternType()),
  SF_PATH_RE_OPT(new PatternType()),
  MIN_FILES_OPT(new UIntType()),
  OUTPUT_COMPRESSION_OPT(new StringType()),
  OUTPUT_BLOCK_SIZE_OPT(new SizeType()),
  OUTPUT_HDFS_BLOCK_SIZE_OPT(new SizeType()),
  OUTPUT_INDEX_BLOCK_SIZE_OPT(new SizeType()),
  OUTPUT_REPLICATION_OPT(new UIntType());

  private Type type;

  private CompactionSettings(Type type) {
    this.type = type;
  }

  public void put(Map<String,String> options, String val) {
    options.put(name(), type.convert(val));
  }
}
