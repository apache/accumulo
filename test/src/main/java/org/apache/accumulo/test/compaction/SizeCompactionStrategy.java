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
package org.apache.accumulo.test.compaction;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;

@SuppressWarnings("removal")
public class SizeCompactionStrategy extends CompactionStrategy {

  private long size = 0;

  @Override
  public void init(Map<String,String> options) {
    size = Long.parseLong(options.get("size"));
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {

    for (DataFileValue dfv : request.getFiles().values()) {
      if (dfv.getSize() < size) {
        return true;
      }
    }

    return false;
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
    CompactionPlan plan = new CompactionPlan();

    for (Entry<StoredTabletFile,DataFileValue> entry : request.getFiles().entrySet()) {
      if (entry.getValue().getSize() < size) {
        plan.inputFiles.add(entry.getKey());
      }
    }

    return plan;
  }

}
