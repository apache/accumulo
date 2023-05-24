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
package org.apache.accumulo.server.manager.state;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;

public class TabletMetadataImposter extends TabletMetadata {

  public TabletMetadataImposter(KeyExtent extent, Location future, Location current, Location last,
      SuspendingTServer suspend, List<LogEntry> walogs, boolean chopped, TabletHostingGoal goal,
      boolean onDemandHostingRequested) {
    super.extent = extent;
    super.endRow = extent.endRow();
    super.tableId = extent.tableId();
    if (current != null && future != null) {
      super.futureAndCurrentLocationSet = true;
      super.location = current;
    } else if (current != null) {
      super.location = current;
    } else if (future != null) {
      super.location = future;
    }
    super.last = last;
    super.suspend = suspend;
    super.logs = walogs == null ? new ArrayList<>() : walogs;
    super.chopped = chopped;
    super.goal = goal;
    super.onDemandHostingRequested = onDemandHostingRequested;
    super.fetchedCols = EnumSet.allOf(ColumnType.class);
  }

}
