/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.monitor.rest.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.GarbageCollection;
import org.apache.accumulo.monitor.rest.api.GarbageCollectorCycle;
import org.apache.accumulo.monitor.rest.api.GarbageCollectorStatus;

/**
 * GarbageCollector metrics
 */
public class GarbageCollectorResource extends BasicResource {

  @GET
  public GarbageCollectorStatus getStatus() {
    return new GarbageCollectorStatus(Monitor.getGcStatus());
  }

  @Path("/files")
  @GET
  public GarbageCollection getFileStatus() {
    GCStatus gcStatus = Monitor.getGcStatus();
    if (null == gcStatus) {
      return GarbageCollection.EMPTY;
    }
    return new GarbageCollection(gcStatus.last, gcStatus.current);
  }

  @Path("/files/last")
  @GET
  public GarbageCollectorCycle getLastCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.EMPTY;
    }
    return new GarbageCollectorCycle(status.last);
  }

  @Path("/files/current")
  @GET
  public GarbageCollectorCycle getCurrentCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.EMPTY;
    }
    return new GarbageCollectorCycle(status.current);
  }

  @Path("/wals")
  @GET
  public GarbageCollection getWalStatus() {
    GCStatus gcStatus = Monitor.getGcStatus();
    if (null == gcStatus) {
      return GarbageCollection.EMPTY;
    }
    return new GarbageCollection(gcStatus.lastLog, gcStatus.currentLog);
  }

  @Path("/wals/last")
  @GET
  public GarbageCollectorCycle getLastWalCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.EMPTY;
    }
    return new GarbageCollectorCycle(status.lastLog);
  }

  @Path("/wals/current")
  @GET
  public GarbageCollectorCycle getCurrentWalCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.EMPTY;
    }
    return new GarbageCollectorCycle(status.currentLog);
  }
}
