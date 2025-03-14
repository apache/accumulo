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
package org.apache.accumulo.manager.split;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.split.FindSplits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeedSplitTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(SeedSplitTask.class);
  private final Manager manager;
  private final KeyExtent extent;

  public SeedSplitTask(Manager manager, KeyExtent extent) {
    this.manager = manager;
    this.extent = extent;
  }

  @Override
  public void run() {
    try {
      var fateInstanceType = FateInstanceType.fromTableId((extent.tableId()));
      manager.fate(fateInstanceType).seedTransaction(Fate.FateOperation.SYSTEM_SPLIT,
          FateKey.forSplit(extent), new FindSplits(extent), true);
    } catch (Exception e) {
      log.error("Failed to split {}", extent, e);
    }
  }

  public KeyExtent getExtent() {
    return extent;
  }
}
