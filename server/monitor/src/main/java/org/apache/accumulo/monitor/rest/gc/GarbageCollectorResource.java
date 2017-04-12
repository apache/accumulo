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
package org.apache.accumulo.monitor.rest.gc;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.monitor.Monitor;

/**
 *
 * GarbageCollector metrics
 *
 * @since 2.0.0
 *
 */
@Path("/gc")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class GarbageCollectorResource {

  /**
   * Returns the garbage collector status
   *
   * @return garbage collector status
   */
  @GET
  public GarbageCollectorStatus getStatus() {
    return new GarbageCollectorStatus(Monitor.getGcStatus());
  }

  /**
   * Generates current and last file gc object
   *
   * @return file gc object
   */
  @Path("files")
  @GET
  public GarbageCollection getFileStatus() {
    GCStatus gcStatus = Monitor.getGcStatus();
    if (null == gcStatus) {
      return GarbageCollection.getEmpty();
    }
    return new GarbageCollection(gcStatus.last, gcStatus.current);
  }

  /**
   * Generates last file gc object
   *
   * @return last file gc object
   */
  @Path("files/last")
  @GET
  public GarbageCollectorCycle getLastCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.getEmpty();
    }
    return new GarbageCollectorCycle(status.last);
  }

  /**
   * Generates current file gc object
   *
   * @return current file gc object
   */
  @Path("files/current")
  @GET
  public GarbageCollectorCycle getCurrentCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.getEmpty();
    }
    return new GarbageCollectorCycle(status.current);
  }

  /**
   * Generates wal gc object
   *
   * @return wal gc object
   */
  @Path("wals")
  @GET
  public GarbageCollection getWalStatus() {
    GCStatus gcStatus = Monitor.getGcStatus();
    if (null == gcStatus) {
      return GarbageCollection.getEmpty();
    }
    return new GarbageCollection(gcStatus.lastLog, gcStatus.currentLog);
  }

  /**
   * Generates last wal object
   *
   * @return last wal object
   */
  @Path("wals/last")
  @GET
  public GarbageCollectorCycle getLastWalCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.getEmpty();
    }
    return new GarbageCollectorCycle(status.lastLog);
  }

  /**
   * Generates current wal object
   *
   * @return current wal object
   */
  @Path("wals/current")
  @GET
  public GarbageCollectorCycle getCurrentWalCycle() {
    GCStatus status = Monitor.getGcStatus();
    if (null == status) {
      return GarbageCollectorCycle.getEmpty();
    }
    return new GarbageCollectorCycle(status.currentLog);
  }
}
