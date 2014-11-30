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

import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.monitor.Monitor;

/**
 * 
 */
@Path("/statistics/time")
@Produces(MediaType.APPLICATION_JSON)
public class StatisticsOverTimeResource {

  @GET
  @Path("scanRate")
  public List<Pair<Long,Integer>> getScanRate() {
    return Monitor.getScanRateOverTime();
  }

  @GET
  @Path("queryRate")
  public List<Pair<Long,Integer>> getQueryRate() {
    return Monitor.getQueryRateOverTime();
  }

  @GET
  @Path("queryByteRate")
  public List<Pair<Long,Double>> getQueryByteRate() {
    return Monitor.getQueryByteRateOverTime();
  }

  @GET
  @Path("load")
  public List<Pair<Long,Double>> getLoad() {
    return Monitor.getLoadOverTime();
  }

  @GET
  @Path("ingestRate")
  public List<Pair<Long,Double>> getIngestRate() {
    return Monitor.getIngestRateOverTime();
  }

  @GET
  @Path("ingestByteRate")
  public List<Pair<Long,Double>> getIngestByteRate() {
    return Monitor.getIngestByteRateOverTime();
  }

  @GET
  @Path("recoveries")
  public List<Pair<Long,Integer>> getRecoveries() {
    return Monitor.getRecoveriesOverTime();
  }

  @GET
  @Path("minorCompactions")
  public List<Pair<Long,Integer>> getMinorCompactions() {
    return Monitor.getMinorCompactionsOverTime();
  }

  @GET
  @Path("majorCompactions")
  public List<Pair<Long,Integer>> getMajorCompactions() {
    return Monitor.getMajorCompactionsOverTime();
  }

  @GET
  @Path("lookups")
  public List<Pair<Long,Double>> getLookups() {
    return Monitor.getLookupsOverTime();
  }
}
