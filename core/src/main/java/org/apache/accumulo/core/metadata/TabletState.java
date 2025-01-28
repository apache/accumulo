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
package org.apache.accumulo.core.metadata;

import java.util.Set;

import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum TabletState {
  UNASSIGNED, ASSIGNED, HOSTED, ASSIGNED_TO_DEAD_SERVER, SUSPENDED;

  private static final Logger log = LoggerFactory.getLogger(TabletState.class);

  public static TabletState compute(TabletMetadata tm, Set<TServerInstance> liveTServers) {
    TabletMetadata.Location current = null;
    TabletMetadata.Location future = null;
    if (tm.hasCurrent()) {
      current = tm.getLocation();
    } else {
      future = tm.getLocation();
    }
    if (future != null) {
      return trace(liveTServers.contains(future.getServerInstance()) ? TabletState.ASSIGNED
          : TabletState.ASSIGNED_TO_DEAD_SERVER, tm);
    } else if (current != null) {
      if (liveTServers.contains(current.getServerInstance())) {
        return trace(TabletState.HOSTED, tm);
      } else {
        return trace(TabletState.ASSIGNED_TO_DEAD_SERVER, tm);
      }
    } else if (tm.getSuspend() != null) {
      return trace(TabletState.SUSPENDED, tm);
    } else {
      return trace(TabletState.UNASSIGNED, tm);
    }
  }

  private static TabletState trace(TabletState tabletState, TabletMetadata tm) {
    // The prev row column for the table may not have been fetched so can not call tm.getExtent()
    log.trace("Computed state of {} for {}", tabletState,
        TabletsSection.encodeRow(tm.getTableId(), tm.getEndRow()));
    return tabletState;
  }
}
