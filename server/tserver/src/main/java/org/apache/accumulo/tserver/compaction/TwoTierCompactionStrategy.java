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
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;

/**
 * A hybrid compaction strategy that supports two types of compression. If total size of files being compacted is less than
 * <tt>table.custom.file.small.compress.threshold</tt> than the faster compression type will be used. The faster compression type is specified in
 * <tt>table.custom.file.small.compress.type</tt>. Otherwise, the normal table compression will be used.
 *
 */
public class TwoTierCompactionStrategy extends DefaultCompactionStrategy {

  /**
   * Threshold memory in bytes. Files smaller than this threshold will use <tt>table.custom.file.small.compress.type</tt> for compression
   */
  public static final String TABLE_SMALL_FILE_COMPRESSION_THRESHOLD = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "file.small.compress.threshold";
  /**
   * Type of compression to use if small threshold is surpassed. One of "gz","lzo","snappy", or "none"
   */
  public static final String TABLE_SMALL_FILE_COMPRESSION_TYPE = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "file.small.compress.type";

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

    String smallFileCompressionType = tableProperties.get(TABLE_SMALL_FILE_COMPRESSION_TYPE);
    String threshold = tableProperties.get(TABLE_SMALL_FILE_COMPRESSION_THRESHOLD);
    verifyRequiredProperties(smallFileCompressionType, threshold);
    Long smallFileCompressionThreshold = AccumuloConfiguration.getMemoryInBytes(threshold);

    long totalSize = 0;
    for (Entry<FileRef,DataFileValue> entry : request.getFiles().entrySet()) {
      totalSize += entry.getValue().getSize();
    }
    if (totalSize < smallFileCompressionThreshold) {
      plan.writeParameters.setCompressType(smallFileCompressionType);
    }
    return plan;
  }

}
