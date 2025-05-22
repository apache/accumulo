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
package org.apache.accumulo.monitor.rest.statistics;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.monitor.Monitor;

/**
 * Generates a list of statistics as a JSON object
 *
 * @since 2.0.0
 */
@Path("/statistics")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class StatisticsResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates the total lookup rate
   *
   * @return Lookup rate
   */
  @GET
  @Path("lookupRate")
  public double getLookupRate() {
    return monitor.getLookupRate();
  }

  /**
   * Generates the total number of tables
   *
   * @return Total number of tables
   */
  @GET
  @Path("totalTables")
  public int getTotalTables() {
    return monitor.getTotalTables();
  }

  /**
   * Generates the total tablet count
   *
   * @return Total tablet count
   */
  @GET
  @Path("totalTabletCount")
  public int getTotalTabletCount() {
    return monitor.getTotalTabletCount();
  }

  /**
   * Generates the total entries
   *
   * @return Total number of entries
   */
  @GET
  @Path("totalEntries")
  public long getTotalEntries() {
    return monitor.getTotalEntries();
  }

  /**
   * Generates the total ingest rate
   *
   * @return Total number of ingest rate
   */
  @GET
  @Path("totalIngestRate")
  public double getTotalIngestRate() {
    return monitor.getTotalIngestRate();
  }

  /**
   * Generates the total query rate
   *
   * @return Total number of query rate
   */
  @GET
  @Path("totalQueryRate")
  public double getTotalQueryRate() {
    return monitor.getTotalQueryRate();
  }

  /**
   * Generates the total scan rate
   *
   * @return Total number of scan rate
   */
  @GET
  @Path("totalScanRate")
  public double getTotalScanRate() {
    return monitor.getTotalScanRate();
  }

  /**
   * Generates the total hold time
   *
   * @return Total hold time
   */
  @GET
  @Path("totalHoldTime")
  public long getTotalHoldTime() {
    return monitor.getTotalHoldTime();
  }

  /**
   * Generates the garbage collector status
   *
   * @return GC status
   */
  @GET
  @Path("gcStatus")
  public GCStatus getGcStatus() {
    return monitor.getGcStatus();
  }

  /**
   * Generates the total lookups
   *
   * @return Total number of lookups
   */
  @GET
  @Path("totalLookups")
  public long getTotalLookups() {
    return monitor.getTotalLookups();
  }
}
