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
package org.apache.accumulo.tserver.compaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.tserver.compaction.strategies.BasicCompactionStrategy;

/**
 * {@link BasicCompactionStrategy} offer the same functionality as this class and more.
 */
// Eclipse might show @SuppressWarnings("removal") as unnecessary.
// Eclipse is wrong. See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565271
@SuppressWarnings("removal")
@Deprecated(since = "2.1.0", forRemoval = true)
public class SizeLimitCompactionStrategy extends DefaultCompactionStrategy {
  public static final String SIZE_LIMIT_OPT = "sizeLimit";

  private long limit;

  @Override
  public void init(Map<String,String> options) {
    limit = ConfigurationTypeHelper.getFixedMemoryAsBytes(options.get(SIZE_LIMIT_OPT));
  }

  private MajorCompactionRequest filterFiles(MajorCompactionRequest mcr) {
    Map<StoredTabletFile,DataFileValue> filteredFiles = new HashMap<>();
    for (Entry<StoredTabletFile,DataFileValue> entry : mcr.getFiles().entrySet()) {
      if (entry.getValue().getSize() <= limit) {
        filteredFiles.put(entry.getKey(), entry.getValue());
      }
    }

    mcr = new MajorCompactionRequest(mcr);
    mcr.setFiles(filteredFiles);

    return mcr;
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
    return super.getCompactionPlan(filterFiles(request));
  }

}
