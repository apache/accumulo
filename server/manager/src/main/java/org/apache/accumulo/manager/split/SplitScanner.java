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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TabletOperation;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Preconditions;

public class SplitScanner implements Runnable {

  private final ExecutorService splitExecutor;
  private final Ample.DataLevel level;
  private final ServerContext context;

  private final Set<KeyExtent> splitting;

  public SplitScanner(ServerContext context, ExecutorService splitExecutor, Ample.DataLevel level) {
    Preconditions.checkArgument(level != Ample.DataLevel.ROOT);
    this.context = context;
    this.splitExecutor = splitExecutor;
    this.level = level;
    splitting = Collections.synchronizedSet(new HashSet<>());
  }

  @Override
  public void run() {
    // TODO fetch less columns?
    var tablets = context.getAmple().readTablets().forLevel(level).build();

    TableId lastTableId = null;
    long threshold = Long.MAX_VALUE;

    for (TabletMetadata tablet : tablets) {
      System.out.println("inspecting for split " + tablet.getExtent());

      // TODO eventually need a way to unload loaded tablets
      if (tablet.getOperation() != TabletOperation.NONE || tablet.getLocation() != null
          || splitting.contains(tablet.getExtent())) {
        System.out.println("ignoring " + tablet.getExtent() + " " + tablet.getOperation() + " "
            + (tablet.getLocation() != null) + " " + splitting.contains(tablet.getExtent()));
        continue;
      }

      if (lastTableId == null || !lastTableId.equals(tablet.getTableId())) {
        System.out.println("getting threshold ");
        threshold = context.getTableConfiguration(tablet.getTableId())
            .getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
        lastTableId = tablet.getTableId();
      }

      System.out.println("threshold " + threshold);

      var tabletSize =
          tablet.getFilesMap().values().stream().mapToLong(DataFileValue::getSize).sum();
      System.out.println("tabletSize " + tabletSize);
      if (tabletSize > threshold) {
        if (splitting.add(tablet.getExtent())) {
          splitExecutor.execute(new SplitTask(context, splitting, tablet));
        }
      }
    }
  }
}
