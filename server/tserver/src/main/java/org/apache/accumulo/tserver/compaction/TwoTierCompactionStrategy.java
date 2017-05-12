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

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.log4j.Logger;

/**
 * A hybrid compaction strategy that supports two types of compression. If total size of files being compacted is larger than
 * <tt>table.majc.compaction.strategy.opts.file.large.compress.threshold</tt> than the larger compression type will be used. The larger compression type is
 * specified in <tt>table.majc.compaction.strategy.opts.file.large.compress.type</tt>. Otherwise, the configured table compression will be used.
 *
 * NOTE: To use this strategy with Minor Compactions set <tt>table.file.compress.type=snappy</tt> and set a different compress type in
 * <tt>table.majc.compaction.strategy.opts.file.large.compress.type</tt> for larger files.
 */
public class TwoTierCompactionStrategy extends DefaultCompactionStrategy {
  private final Logger log = Logger.getLogger(TwoTierCompactionStrategy.class);
  /**
   * Threshold memory in bytes. Files larger than this threshold will use <tt>table.majc.compaction.strategy.opts.file.large.compress.type</tt> for compression
   */
  public static final String LARGE_FILE_COMPRESSION_THRESHOLD = "file.large.compress.threshold";
  private Long largeFileCompressionThreshold;

  /**
   * Type of compression to use if large threshold is surpassed. One of "gz","lzo","snappy", or "none"
   */
  public static final String LARGE_FILE_COMPRESSION_TYPE = "file.large.compress.type";
  private String largeFileCompressionType;

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
        throw new IllegalArgumentException("Missing required " + Property.TABLE_COMPACTION_STRATEGY_PREFIX + " (" + LARGE_FILE_COMPRESSION_TYPE + " and/or "
            + LARGE_FILE_COMPRESSION_THRESHOLD + ") for " + this.getClass().getName());
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
  public void init(Map<String,String> options) {
    String threshold = options.get(LARGE_FILE_COMPRESSION_THRESHOLD);
    largeFileCompressionType = options.get(LARGE_FILE_COMPRESSION_TYPE);
    verifyRequiredProperties(threshold, largeFileCompressionType);
    largeFileCompressionThreshold = ConfigurationTypeHelper.getFixedMemoryAsBytes(threshold);
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
    Long totalSize = calculateTotalSize(request, plan);

    if (totalSize > largeFileCompressionThreshold) {
      if (log.isDebugEnabled()) {
        log.debug("Changed compressType to " + largeFileCompressionType + ": totalSize(" + totalSize + ") was greater than threshold "
            + largeFileCompressionThreshold);
      }
      plan.writeParameters.setCompressType(largeFileCompressionType);
    }
    return plan;
  }

}
