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

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.monitor.Monitor;

/**
 *
 */
@Path("/statistics")
@Produces(MediaType.APPLICATION_JSON)
public class StatisticsResource {

  @GET
  @Path("lookupRate")
  public double getLookupRate() {
    return Monitor.getLookupRate();
  }

  @GET
  @Path("totalTables")
  public int getTotalTables() {
    return Monitor.getTotalTables();
  }

  @GET
  @Path("totalTabletCount")
  public int getTotalTabletCount() {
    return Monitor.getTotalTabletCount();
  }

  @GET
  @Path("totalEntries")
  public long getTotalEntries() {
    return Monitor.getTotalEntries();
  }

  @GET
  @Path("totalIngestRate")
  public double getTotalIngestRate() {
    return Monitor.getTotalIngestRate();
  }

  @GET
  @Path("totalQueryRate")
  public double getTotalQueryRate() {
    return Monitor.getTotalQueryRate();
  }

  @GET
  @Path("totalScanRate")
  public double getTotalScanRate() {
    return Monitor.getTotalScanRate();
  }

  @GET
  @Path("totalHoldTime")
  public long getTotalHoldTime() {
    return Monitor.getTotalHoldTime();
  }

  @GET
  @Path("gcStatus")
  public GCStatus getGcStatus() {
    return Monitor.getGcStatus();
  }

  @GET
  @Path("totalLookups")
  public long getTotalLookups() {
    return Monitor.getTotalLookups();
  }

  @GET
  @Path("time/scanRate")
  public List<Pair<Long,Integer>> getScanRate() {
    return Monitor.getScanRateOverTime();
  }

  @GET
  @Path("time/queryRate")
  public List<Pair<Long,Integer>> getQueryRate() {
    return Monitor.getQueryRateOverTime();
  }

  @GET
  @Path("time/queryByteRate")
  public List<Pair<Long,Double>> getQueryByteRate() {
    return Monitor.getQueryByteRateOverTime();
  }

  @GET
  @Path("time/load")
  public List<Pair<Long,Double>> getLoad() {
    return Monitor.getLoadOverTime();
  }

  @GET
  @Path("time/ingestRate")
  public List<Pair<Long,Double>> getIngestRate() {
    return Monitor.getIngestRateOverTime();
  }

  @GET
  @Path("time/ingestByteRate")
  public List<Pair<Long,Double>> getIngestByteRate() {
    return Monitor.getIngestByteRateOverTime();
  }

  @GET
  @Path("time/minorCompactions")
  public List<Pair<Long,Integer>> getMinorCompactions() {
    return Monitor.getMinorCompactionsOverTime();
  }

  @GET
  @Path("time/majorCompactions")
  public List<Pair<Long,Integer>> getMajorCompactions() {
    return Monitor.getMajorCompactionsOverTime();
  }

  @GET
  @Path("time/lookups")
  public List<Pair<Long,Double>> getLookups() {
    return Monitor.getLookupsOverTime();
  }
}
