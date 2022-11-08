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

import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.util.Pair;
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

  /**
   * Generates a list with the scan rate over time
   *
   * @return Scan rate over time
   */
  @GET
  @Path("time/scanRate")
  public List<Pair<Long,Long>> getScanRate() {
    return monitor.getScanRateOverTime();
  }

  /**
   * Generates a list with the query rate over time
   *
   * @return Query rate over time
   */
  @GET
  @Path("time/queryRate")
  public List<Pair<Long,Long>> getQueryRate() {
    return monitor.getQueryRateOverTime();
  }

  /**
   * Generates a list with the scan entries over time
   *
   * @return Scan entries over time
   */
  @GET
  @Path("time/scanEntries")
  public List<Pair<String,List<Pair<Long,Long>>>> getScanEntries() {

    List<Pair<String,List<Pair<Long,Long>>>> scanEntries = new ArrayList<>();

    Pair<String,List<Pair<Long,Long>>> read = new Pair<>("Read", monitor.getScanRateOverTime());
    Pair<String,List<Pair<Long,Long>>> returned =
        new Pair<>("Returned", monitor.getQueryRateOverTime());

    scanEntries.add(read);
    scanEntries.add(returned);

    return scanEntries;
  }

  /**
   * Generates a list with the query byte rate over time
   *
   * @return Query byte rate over time
   */
  @GET
  @Path("time/queryByteRate")
  public List<Pair<Long,Double>> getQueryByteRate() {
    return monitor.getQueryByteRateOverTime();
  }

  /**
   * Generates a list with the load over time
   *
   * @return Load over time
   */
  @GET
  @Path("time/load")
  public List<Pair<Long,Double>> getLoad() {
    return monitor.getLoadOverTime();
  }

  /**
   * Generates a list with the ingest rate over time
   *
   * @return Ingest rate over time
   */
  @GET
  @Path("time/ingestRate")
  public List<Pair<Long,Double>> getIngestRate() {
    return monitor.getIngestRateOverTime();
  }

  /**
   * Generates a list with the ingest byte rate over time
   *
   * @return Ingest byte rate over time
   */
  @GET
  @Path("time/ingestByteRate")
  public List<Pair<Long,Double>> getIngestByteRate() {
    return monitor.getIngestByteRateOverTime();
  }

  /**
   * Generates a list with the minor compactions over time
   *
   * @return Minor compactions over time
   */
  @GET
  @Path("time/minorCompactions")
  public List<Pair<Long,Integer>> getMinorCompactions() {
    return monitor.getMinorCompactionsOverTime();
  }

  /**
   * Generates a list with the major compactions over time
   *
   * @return Major compactions over time
   */
  @GET
  @Path("time/majorCompactions")
  public List<Pair<Long,Integer>> getMajorCompactions() {
    return monitor.getMajorCompactionsOverTime();
  }

  /**
   * Generates a list with the lookups over time
   *
   * @return Lookups over time
   */
  @GET
  @Path("time/lookups")
  public List<Pair<Long,Double>> getLookups() {
    return monitor.getLookupsOverTime();
  }

  /**
   * Generates a list with the index cache hit rate over time
   *
   * @return Index cache hit rate over time
   */
  @GET
  @Path("time/indexCacheHitRate")
  public List<Pair<Long,Double>> getIndexCacheHitRate() {
    return monitor.getIndexCacheHitRateOverTime();
  }

  /**
   * Generates a list with the data cache hit rate over time
   *
   * @return Data cache hit rate over time
   */
  @GET
  @Path("time/dataCacheHitRate")
  public List<Pair<Long,Double>> getDataCacheHitRate() {
    return monitor.getDataCacheHitRateOverTime();
  }
}
