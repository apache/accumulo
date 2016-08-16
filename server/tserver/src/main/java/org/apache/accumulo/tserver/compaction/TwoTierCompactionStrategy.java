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
package org.apache.accumulo.tserver.compaction;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;

/**
 * A hybrid compaction strategy that supports two types of compression. If total size of files being compacted is larger than
 * <tt>table.custom.file.large.compress.threshold</tt> than the larger compression type will be used. The larger compression type is specified in
 * <tt>table.custom.file.large.compress.type</tt>. Otherwise, the configured table compression will be used.
 *
 * NOTE: To use this strategy with Minor Compactions set <tt>table.file.compress.type=snappy</tt> and set a different compress type in
 * <tt>table.custom.file.large.compress.type</tt> for larger files.
 */
public class TwoTierCompactionStrategy extends DefaultCompactionStrategy {

  /**
   * Threshold memory in bytes. Files larger than this threshold will use <tt>table.custom.file.large.compress.type</tt> for compression
   */
  public static final String TABLE_LARGE_FILE_COMPRESSION_THRESHOLD = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "file.large.compress.threshold";
  /**
   * Type of compression to use if large threshold is surpassed. One of "gz","lzo","snappy", or "none"
   */
  public static final String TABLE_LARGE_FILE_COMPRESSION_TYPE = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "file.large.compress.type";

  /**
   * Helper method to check for required table properties.
   *
   * @param objectsToVerify
   *          any objects that shouldn't be null
   * @throws IllegalArgumentException
   *           if any object in {@code objectsToVerify} is null
   *
   */
  public void verifyRequiredProperties(Object... objectsToVerify) throws IllegalArgumentException {
    for (Object obj : objectsToVerify) {
      if (obj == null) {
        throw new IllegalArgumentException("Missing required Table properties for " + this.getClass().getName());
      }
    }
  }

  /**
   * Calculates the total size of input files in the compaction plan
   */
  private Long calculateTotalSize(MajorCompactionRequest request, CompactionPlan plan) {
    long totalSize = 0;
    Map<FileRef,DataFileValue> allFiles = request.getFiles();
    for (FileRef fileRef : plan.inputFiles) {
      totalSize += allFiles.get(fileRef).getSize();
    }
    return totalSize;
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {
    return super.shouldCompact(request);
  }

  @Override
  public void gatherInformation(MajorCompactionRequest request) throws IOException {
    super.gatherInformation(request);
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
    CompactionPlan plan = super.getCompactionPlan(request);
    plan.writeParameters = new WriteParameters();
    Map<String,String> tableProperties = request.getTableProperties();
    verifyRequiredProperties(tableProperties);

    String largeFileCompressionType = tableProperties.get(TABLE_LARGE_FILE_COMPRESSION_TYPE);
    String threshold = tableProperties.get(TABLE_LARGE_FILE_COMPRESSION_THRESHOLD);
    verifyRequiredProperties(largeFileCompressionType, threshold);
    Long largeFileCompressionThreshold = AccumuloConfiguration.getMemoryInBytes(threshold);
    Long totalSize = calculateTotalSize(request, plan);

    if (totalSize > largeFileCompressionThreshold) {
      plan.writeParameters.setCompressType(largeFileCompressionType);
    }
    return plan;
  }

}
