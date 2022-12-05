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

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;

@SuppressWarnings("removal")
public class TestCompactionStrategy extends CompactionStrategy {

  private String inputPrefix = "Z";
  private String dropPrefix = "Z";
  private boolean shouldCompact = false;

  @Override
  public void init(Map<String,String> options) {
    if (options.containsKey("inputPrefix")) {
      inputPrefix = options.get("inputPrefix");
    }
    if (options.containsKey("dropPrefix")) {
      dropPrefix = options.get("dropPrefix");
    }
    if (options.containsKey("shouldCompact")) {
      shouldCompact = Boolean.parseBoolean(options.get("shouldCompact"));
    }
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {
    if (shouldCompact) {
      return true;
    }

    for (TabletFile file : request.getFiles().keySet()) {
      if (file.getFileName().startsWith(inputPrefix)) {
        return true;
      }
      if (file.getFileName().startsWith(dropPrefix)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
    CompactionPlan plan = new CompactionPlan();

    for (StoredTabletFile file : request.getFiles().keySet()) {
      if (file.getFileName().startsWith(dropPrefix)) {
        plan.deleteFiles.add(file);
      } else if (file.getFileName().startsWith(inputPrefix)) {
        plan.inputFiles.add(file);
      }
    }

    return plan;
  }
}
