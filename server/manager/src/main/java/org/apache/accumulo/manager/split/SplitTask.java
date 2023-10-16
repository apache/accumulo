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

import java.time.Duration;
import java.util.SortedSet;

import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.split.PreSplit;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(SplitTask.class);
  private final Manager manager;

  private final ServerContext context;
  private TabletMetadata tablet;
  private final long creationTime;

  public SplitTask(ServerContext context, TabletMetadata tablet, Manager manager) {
    this.context = context;
    this.tablet = tablet;
    this.manager = manager;
    this.creationTime = System.nanoTime();
  }

  @Override
  public void run() {
    try {
      if (Duration.ofNanos(System.nanoTime() - creationTime).compareTo(Duration.ofMinutes(2)) > 0) {
        // the tablet was in the thread pool queue for a bit, lets reread its metadata
        tablet = manager.getContext().getAmple().readTablet(tablet.getExtent());
        if (tablet == null) {
          // the tablet no longer exists
          return;
        }
      }

      if (tablet.getOperationId() != null) {
        // This will be checked in the FATE op, but no need to inspect files and start a FATE op if
        // it currently has an operation running against it.
        log.debug("Not splitting {} because it has operation id {}", tablet.getExtent(),
            tablet.getOperationId());
        manager.getSplitter().removeSplitStarting(tablet.getExtent());
        return;
      }

      var extent = tablet.getExtent();

      SortedSet<Text> splits = SplitUtils.findSplits(context, tablet);

      if (tablet.getEndRow() != null) {
        splits.remove(tablet.getEndRow());
      }

      if (splits.size() == 0) {
        log.info("Tablet {} needs to split, but no split points could be found.",
            tablet.getExtent());

        manager.getSplitter().rememberUnsplittable(tablet);
        manager.getSplitter().removeSplitStarting(tablet.getExtent());
        return;
      }

      long fateTxId = manager.fate().startTransaction();

      manager.fate().seedTransaction("SYSTEM_SPLIT", fateTxId, new PreSplit(extent, splits), true,
          "System initiated split of tablet " + extent + " into " + splits.size() + " splits");
    } catch (Exception e) {
      log.error("Failed to split {}", tablet.getExtent(), e);
    }
  }
}
