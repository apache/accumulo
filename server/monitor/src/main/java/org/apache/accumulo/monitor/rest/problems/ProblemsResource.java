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
package org.apache.accumulo.monitor.rest.problems;

import static org.apache.accumulo.monitor.util.ParameterValidator.ALPHA_NUM_REGEX;
import static org.apache.accumulo.monitor.util.ParameterValidator.RESOURCE_REGEX;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Generates a problem summary and details as a JSON object
 *
 * @since 2.0.0
 *
 */
@Path("/problems")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class ProblemsResource {

  /**
   * Generates a list with the problem summary
   *
   * @return problem summary list
   */
  @GET
  @Path("summary")
  public ProblemSummary getSummary() {

    ProblemSummary problems = new ProblemSummary();

    if (Monitor.getProblemException() == null) {
      for (Entry<Table.ID,Map<ProblemType,Integer>> entry : Monitor.getProblemSummary().entrySet()) {
        Integer readCount = null, writeCount = null, loadCount = null;

        for (ProblemType pt : ProblemType.values()) {
          Integer pcount = entry.getValue().get(pt);
          if (pt.equals(ProblemType.FILE_READ)) {
            readCount = pcount;
          } else if (pt.equals(ProblemType.FILE_WRITE)) {
            writeCount = pcount;
          } else if (pt.equals(ProblemType.TABLET_LOAD)) {
            loadCount = pcount;
          }
        }

        String tableName = Tables.getPrintableTableInfoFromId(HdfsZooInstance.getInstance(), entry.getKey());

        problems.addProblemSummary(new ProblemSummaryInformation(tableName, entry.getKey(), readCount, writeCount, loadCount));
      }
    }
    return problems;
  }

  /**
   * REST call to clear problem reports from a table
   *
   * @param tableID
   *          Table ID to clear problems
   */
  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  @Path("summary")
  public void clearTableProblems(@QueryParam("s") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX) String tableID) {
    Logger log = LoggerFactory.getLogger(Monitor.class);
    try {
      ProblemReports.getInstance(Monitor.getContext()).deleteProblemReports(Table.ID.of(tableID));
    } catch (Exception e) {
      log.error("Failed to delete problem reports for table " + (StringUtils.isEmpty(tableID) ? StringUtils.EMPTY : tableID), e);
    }
  }

  /**
   * Generates a list of the problem details as a JSON object
   *
   * @return problem details list
   */
  @GET
  @Path("details")
  public ProblemDetail getDetails() {

    ProblemDetail problems = new ProblemDetail();

    if (Monitor.getProblemException() == null) {
      for (Entry<Table.ID,Map<ProblemType,Integer>> entry : Monitor.getProblemSummary().entrySet()) {
        ArrayList<ProblemReport> problemReports = new ArrayList<>();
        Iterator<ProblemReport> iter = entry.getKey() == null ? ProblemReports.getInstance(Monitor.getContext()).iterator() : ProblemReports.getInstance(
            Monitor.getContext()).iterator(entry.getKey());
        while (iter.hasNext())
          problemReports.add(iter.next());
        for (ProblemReport pr : problemReports) {
          String tableName = Tables.getPrintableTableInfoFromId(HdfsZooInstance.getInstance(), pr.getTableId());

          problems.addProblemDetail(new ProblemDetailInformation(tableName, entry.getKey(), pr.getProblemType().name(), pr.getServer(), pr.getTime(), pr
              .getResource(), pr.getException()));
        }
      }
    }
    return problems;
  }

  /**
   * REST call to clear specific problem details
   *
   * @param tableID
   *          Table ID to clear
   * @param resource
   *          Resource to clear
   * @param ptype
   *          Problem type to clear
   */
  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  @Path("details")
  public void clearDetailsProblems(@QueryParam("table") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX) String tableID, @QueryParam("resource") @NotNull @Pattern(
      regexp = RESOURCE_REGEX) String resource, @QueryParam("ptype") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX) String ptype) {
    Logger log = LoggerFactory.getLogger(Monitor.class);
    try {
      ProblemReports.getInstance(Monitor.getContext()).deleteProblemReport(Table.ID.of(tableID), ProblemType.valueOf(ptype), resource);
    } catch (Exception e) {
      log.error("Failed to delete problem reports for table " + (StringUtils.isBlank(tableID) ? "" : tableID), e);
    }
  }

  @GET
  @Path("exception")
  public Exception getException() {
    return Monitor.getProblemException();
  }

}
