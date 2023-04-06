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
package org.apache.accumulo.core.manager.balancer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;

public class AssignmentParamsImpl implements TabletBalancer.AssignmentParameters {
  private final SortedMap<TabletServerId,TServerStatus> currentStatus;
  private final Map<TabletId,TabletServerId> unassigned;
  private final Map<TabletId,TabletServerId> assignmentsOut;
  private final SortedMap<TServerInstance,TabletServerStatus> thriftCurrentStatus;
  private final Map<KeyExtent,TServerInstance> thriftUnassigned;
  private final Map<KeyExtent,TServerInstance> thriftAssignmentsOut;

  public static AssignmentParamsImpl fromThrift(
      SortedMap<TServerInstance,TabletServerStatus> currentStatus,
      Map<KeyExtent,TServerInstance> unassigned, Map<KeyExtent,TServerInstance> assignmentsOut) {

    SortedMap<TabletServerId,TServerStatus> currentStatusNew = new TreeMap<>();
    currentStatus.forEach((tsi, status) -> currentStatusNew.put(new TabletServerIdImpl(tsi),
        TServerStatusImpl.fromThrift(status)));
    Map<TabletId,TabletServerId> unassignedNew = new HashMap<>();
    unassigned.forEach(
        (ke, tsi) -> unassignedNew.put(new TabletIdImpl(ke), TabletServerIdImpl.fromThrift(tsi)));

    return new AssignmentParamsImpl(Collections.unmodifiableSortedMap(currentStatusNew),
        Collections.unmodifiableMap(unassignedNew), currentStatus, unassigned, assignmentsOut);
  }

  public AssignmentParamsImpl(SortedMap<TabletServerId,TServerStatus> currentStatus,
      Map<TabletId,TabletServerId> unassigned, Map<TabletId,TabletServerId> assignmentsOut) {
    this.currentStatus = currentStatus;
    this.unassigned = unassigned;
    this.assignmentsOut = assignmentsOut;
    this.thriftCurrentStatus = null;
    this.thriftUnassigned = null;
    this.thriftAssignmentsOut = null;
  }

  private AssignmentParamsImpl(SortedMap<TabletServerId,TServerStatus> currentStatus,
      Map<TabletId,TabletServerId> unassigned,
      SortedMap<TServerInstance,TabletServerStatus> thriftCurrentStatus,
      Map<KeyExtent,TServerInstance> thriftUnassigned,
      Map<KeyExtent,TServerInstance> thriftAssignmentsOut) {
    this.currentStatus = currentStatus;
    this.unassigned = unassigned;
    this.assignmentsOut = null;
    this.thriftCurrentStatus = thriftCurrentStatus;
    this.thriftUnassigned = thriftUnassigned;
    this.thriftAssignmentsOut = thriftAssignmentsOut;
  }

  @Override
  public SortedMap<TabletServerId,TServerStatus> currentStatus() {
    return currentStatus;
  }

  @Override
  public Map<TabletId,TabletServerId> unassignedTablets() {
    return unassigned;
  }

  @Override
  public void addAssignment(TabletId tabletId, TabletServerId tabletServerId) {
    if (assignmentsOut != null) {
      assignmentsOut.put(tabletId, tabletServerId);
    }
    if (thriftAssignmentsOut != null) {
      thriftAssignmentsOut.put(KeyExtent.fromTabletId(tabletId),
          TabletServerIdImpl.toThrift(tabletServerId));
    }
  }

  public SortedMap<TServerInstance,TabletServerStatus> thriftCurrentStatus() {
    return thriftCurrentStatus;
  }

  public Map<KeyExtent,TServerInstance> thriftUnassigned() {
    return thriftUnassigned;
  }

  public Map<KeyExtent,TServerInstance> thriftAssignmentsOut() {
    return thriftAssignmentsOut;
  }
}
