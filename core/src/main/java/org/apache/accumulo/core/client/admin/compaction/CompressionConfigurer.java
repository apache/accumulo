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
package org.apache.accumulo.core.client.admin.compaction;

import java.util.Map;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;

/**
 * A compaction configurer that can adjust the compression configuration for a compaction when the
 * sum of the input file sizes exceeds a threshold.
 *
 * <p>
 * To use compression type CL for large files and CS for small files, set the following table
 * properties.
 *
 * <ul>
 * <li>Set {@code table.compaction.configurer} to the fully qualified name of this class.
 * <li>Set {@code table.compaction.configurer.opts.large.compress.threshold } to a size in bytes.
 * The suffixes K,M,and G can be used. When the inputs exceed this size, the following compression
 * is used.
 * <li>Set {@code  table.compaction.configurer.opts.large.compress.type} to CL.
 * <li>Set {@code table.file.compress.type=CS}
 * </ul>
 *
 * <p>
 * With the above config, minor compaction and small compaction will use CS for compression.
 * Everything else will use CL. For example CS could be snappy and CL could be gzip. Using a faster
 * compression for small files and slower compression for larger files can increase ingestion
 * throughput without using a lot of extra space.
 *
 * @since 2.1.0
 */
public class CompressionConfigurer implements CompactionConfigurer {

  public static final String LARGE_FILE_COMPRESSION_THRESHOLD = "large.compress.threshold";

  public static final String LARGE_FILE_COMPRESSION_TYPE = "large.compress.type";

  private Long largeThresh;
  private String largeCompress;

  @Override
  public void init(InitParameters iparams) {
    var options = iparams.getOptions();

    String largeThresh = options.get(LARGE_FILE_COMPRESSION_THRESHOLD);
    String largeCompress = options.get(LARGE_FILE_COMPRESSION_TYPE);
    if (largeThresh != null && largeCompress != null) {
      this.largeThresh = ConfigurationTypeHelper.getFixedMemoryAsBytes(largeThresh);
      this.largeCompress = largeCompress;
    } else if (largeThresh != null ^ largeCompress != null) {
      throw new IllegalArgumentException(
          "Must set both of " + Property.TABLE_COMPACTION_CONFIGURER_OPTS.getKey() + " ("
              + LARGE_FILE_COMPRESSION_TYPE + " and " + LARGE_FILE_COMPRESSION_THRESHOLD
              + ") or neither for " + this.getClass().getName());
    }
  }

  @Override
  public Overrides override(InputParameters params) {
    long inputsSum =
        params.getInputFiles().stream().mapToLong(CompactableFile::getEstimatedSize).sum();

    if (inputsSum > largeThresh) {
      return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), largeCompress));
    }

    return new Overrides(Map.of());
  }

}
