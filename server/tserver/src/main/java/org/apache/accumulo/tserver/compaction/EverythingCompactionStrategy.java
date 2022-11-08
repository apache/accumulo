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

/**
 * The default compaction strategy for user initiated compactions. This strategy will always select
 * all files.
 */
// Eclipse might show @SuppressWarnings("removal") as unnecessary.
// Eclipse is wrong. See https://bugs.eclipse.org/bugs/show_bug.cgi?id=565271
@SuppressWarnings("removal")
@Deprecated(since = "2.1.0", forRemoval = true)
public class EverythingCompactionStrategy extends CompactionStrategy {

  @Override
  public boolean shouldCompact(MajorCompactionRequest request) {
    return true; // ACCUMULO-3645 compact for empty files too
  }

  @Override
  public CompactionPlan getCompactionPlan(MajorCompactionRequest request) {
    CompactionPlan plan = new CompactionPlan();
    plan.inputFiles.addAll(request.getFiles().keySet());
    return plan;
  }
}
