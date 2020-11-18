/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.state;

import org.apache.accumulo.core.metadata.TabletState;

public class TableCounts {
  int[] counts = new int[TabletState.values().length];

  public int unassigned() {
    return counts[TabletState.UNASSIGNED.ordinal()];
  }

  public int assigned() {
    return counts[TabletState.ASSIGNED.ordinal()];
  }

  public int assignedToDeadServers() {
    return counts[TabletState.ASSIGNED_TO_DEAD_SERVER.ordinal()];
  }

  public int hosted() {
    return counts[TabletState.HOSTED.ordinal()];
  }

  public int suspended() {
    return counts[TabletState.SUSPENDED.ordinal()];
  }
}
