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

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

// ELASTICITY_TODO explore moving this functionality into TabletGroupWatcher (also consider the
// compaction scan).  Could we do server side filtering to find candidates?  In addition to or
// independently of moving code to the tablet group watcher, we could sum up files sizes on the
// tablet server using an accumulo iterator and return on the sum.
public class SplitScanner implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(SplitScanner.class);

  private final Ample.DataLevel level;
  private final ServerContext context;

  private final Manager manager;

  public SplitScanner(ServerContext context, Ample.DataLevel level, Manager manager) {
    Preconditions.checkArgument(level != Ample.DataLevel.ROOT);
    this.context = context;
    this.level = level;
    this.manager = manager;
  }

  @Override
  public void run() {
    var tablets = context.getAmple().readTablets().forLevel(level)
        .fetch(ColumnType.FILES, ColumnType.OPID, ColumnType.PREV_ROW, ColumnType.LOCATION).build();

    TableId lastTableId = null;
    long threshold = Long.MAX_VALUE;

    boolean isOnline = true;

    for (TabletMetadata tablet : tablets) {
      if (tablet.getOperationId() != null || !manager.getSplitter().shouldInspect(tablet)) {
        log.debug("ignoring for split inspection {} {} {}", tablet.getExtent(),
            tablet.getOperationId(), !manager.getSplitter().shouldInspect(tablet));
        continue;
      }

      if (lastTableId == null || !lastTableId.equals(tablet.getTableId())) {
        threshold = context.getTableConfiguration(tablet.getTableId())
            .getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
        lastTableId = tablet.getTableId();

        isOnline = context.getTableState(lastTableId) == TableState.ONLINE;
      }

      if (!isOnline) {
        continue;
      }

      var tabletSize =
          tablet.getFilesMap().values().stream().mapToLong(DataFileValue::getSize).sum();

      if (tabletSize > threshold) {
        if (manager.getSplitter().addSplitStarting(tablet.getExtent())) {
          log.debug("submitting for split tablet:{} size:{} threshold:{}", tablet.getExtent(),
              tabletSize, threshold);
          manager.getSplitter().executeSplit(new SplitTask(context, tablet, manager));
        }
      }
    }
  }
}
