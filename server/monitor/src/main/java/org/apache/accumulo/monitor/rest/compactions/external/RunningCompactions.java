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
package org.apache.accumulo.monitor.rest.compactions.external;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;

public class RunningCompactions {

  public final List<RunningCompactionInfo> running = new ArrayList<>();

  public RunningCompactions(Map<String,TExternalCompaction> rMap) {
    if (rMap != null) {
      for (var entry : rMap.entrySet()) {
        running.add(new RunningCompactionInfo(entry.getValue()));
      }
    }
  }
}
