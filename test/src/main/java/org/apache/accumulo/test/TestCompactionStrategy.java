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
package org.apache.accumulo.test;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;

public class TestCompactionStrategy extends CompactionStrategy {

  private String inputPrefix = "Z";
  private String dropPrefix = "Z";
  private boolean shouldCompact = false;

  @Override
  public void init(Map<String,String> options) {
    if (options.containsKey("inputPrefix"))
      inputPrefix = options.get("inputPrefix");
    if (options.containsKey("dropPrefix"))
      dropPrefix = options.get("dropPrefix");
    if (options.containsKey("shouldCompact"))
      shouldCompact = Boolean.parseBoolean(options.get("shouldCompact"));
  }

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) throws IOException {
    if (shouldCompact)
      return true;

    for (FileRef fref : request.getFiles().keySet()) {
      if (fref.path().getName().startsWith(inputPrefix))
        return true;
      if (fref.path().getName().startsWith(dropPrefix))
        return true;
    }

    return false;
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) throws IOException {
    CompactionPlan plan = new CompactionPlan();

    for (FileRef fref : request.getFiles().keySet()) {
      if (fref.path().getName().startsWith(dropPrefix)) {
        plan.deleteFiles.add(fref);
      } else if (fref.path().getName().startsWith(inputPrefix)) {
        plan.inputFiles.add(fref);
      }
    }

    return plan;
  }
}
