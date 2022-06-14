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
package org.apache.accumulo.tserver.compaction.strategies;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.admin.compaction.CompressionConfigurer;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A compaction strategy that covers the following uses cases.
 *
 * <ul>
 * <li>Filtering out input files larger than a specified size. These are never considered for
 * compaction.
 * <li>Compressing output files differently when the sum of the input files exceeds a specified
 * size.
 * </ul>
 *
 * <p>
 * To filter out input files based on size set
 * {@code table.majc.compaction.strategy.opts.filter.size} to the desired size.
 *
 * <p>
 * To use a different compression for larger inputs set
 * {@code table.majc.compaction.strategy.opts.large.compress.threshold } to bytes and
 * {@code  table.majc.compaction.strategy.opts.large.compress.type} to a compression type like gz or
 * snappy. When setting one of these properties then the other must be set. When the total size of
 * files being compacted is larger than the threshold then the specified compression type is used.
 *
 * <p>
 * To use this strategy with Minor Compactions set {@code table.file.compress.type=snappy} and set a
 * different compress type in {@code table.majc.compaction.strategy.opts.large.compress.type} for
 * larger files.
 *
 * <p>
 * The options that take sizes are in bytes and the suffixes K,M,and G can be used.
 *
 * @deprecated since 2.1.0 see {@link CompressionConfigurer}. Also compaction planners introduced in
 *             2.1.0 have the ability to avoid compacting files over a certain size. See
 *             {@link DefaultCompactionPlanner}
 */
// Eclipse might show @SuppressWarnings("removal") as unnecessary.
// Eclipse is wrong. See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565271
@SuppressWarnings("removal")
@Deprecated(since = "2.1.0", forRemoval = true)
public class BasicCompactionStrategy extends DefaultCompactionStrategy {

  private static final Logger log = LoggerFactory.getLogger(BasicCompactionStrategy.class);

  public static final String SIZE_LIMIT_OPT = "filter.size";

  /**
   * Threshold memory in bytes. Files larger than this threshold will use
   * <code>table.majc.compaction.strategy.opts.file.large.compress.type</code> for compression
   */
  public static final String LARGE_FILE_COMPRESSION_THRESHOLD = "large.compress.threshold";

  /**
   * Type of compression to use if large threshold is surpassed. One of "none", "gz", "bzip2",
   * "lzo", "lz4", "snappy", or "zstd"
   */
  public static final String LARGE_FILE_COMPRESSION_TYPE = "large.compress.type";

  private Long filterSize;
  private Long largeThresh;
  private String largeCompress;

  @Override
  public void init(Map<String,String> options) {
    String limitVal = options.get(SIZE_LIMIT_OPT);
    if (limitVal != null) {
      filterSize = ConfigurationTypeHelper.getFixedMemoryAsBytes(limitVal);
    }

    String largeThresh = options.get(LARGE_FILE_COMPRESSION_THRESHOLD);
    String largeCompress = options.get(LARGE_FILE_COMPRESSION_TYPE);
    if (largeThresh != null && largeCompress != null) {
      this.largeThresh = ConfigurationTypeHelper.getFixedMemoryAsBytes(largeThresh);
      this.largeCompress = largeCompress;
    } else if (largeThresh != null ^ largeCompress != null) {
      throw new IllegalArgumentException("Must set both of "
          + Property.TABLE_COMPACTION_STRATEGY_PREFIX + " (" + LARGE_FILE_COMPRESSION_TYPE + " and "
          + LARGE_FILE_COMPRESSION_THRESHOLD + ") or neither for " + this.getClass().getName());
    }

  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {
    return super.shouldCompact(filterFiles(request));
  }

  @Override
  public void gatherInformation(MajorCompactionRequest request) throws IOException {
    super.gatherInformation(filterFiles(request));
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {

    request = filterFiles(request);

    CompactionPlan plan = super.getCompactionPlan(request);

    if (largeThresh != null) {

      Long totalSize = calculateTotalSize(request, plan);

      if (totalSize > largeThresh) {
        plan.writeParameters = new WriteParameters();
        if (log.isDebugEnabled()) {
          log.debug("Changed compressType to {}: totalSize({}) was greater than threshold {}",
              largeCompress, totalSize, largeThresh);
        }
        plan.writeParameters.setCompressType(largeCompress);
      }
    }

    return plan;

  }

  private MajorCompactionRequest filterFiles(MajorCompactionRequest mcr) {
    if (filterSize != null) {
      Map<StoredTabletFile,DataFileValue> filteredFiles = new HashMap<>();
      mcr.getFiles().forEach((fr, dfv) -> {
        if (dfv.getSize() <= filterSize) {
          filteredFiles.put(fr, dfv);
        }
      });

      mcr = new MajorCompactionRequest(mcr);
      mcr.setFiles(filteredFiles);
    }
    return mcr;
  }

  /**
   * Calculates the total size of input files in the compaction plan
   */
  private Long calculateTotalSize(MajorCompactionRequest request, CompactionPlan plan) {
    long totalSize = 0;
    Map<StoredTabletFile,DataFileValue> allFiles = request.getFiles();
    for (StoredTabletFile fileRef : plan.inputFiles) {
      totalSize += allFiles.get(fileRef).getSize();
    }
    return totalSize;
  }
}
