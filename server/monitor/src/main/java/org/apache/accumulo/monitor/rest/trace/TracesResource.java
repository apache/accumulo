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
package org.apache.accumulo.monitor.rest.trace;

import static org.apache.accumulo.monitor.util.ParameterValidator.ALPHA_NUM_REGEX;
import static org.apache.accumulo.monitor.util.ParameterValidator.RESOURCE_REGEX;

import jakarta.inject.Inject;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.monitor.Monitor;

/**
 * Generates a list of traces with the summary, by type, and trace details
 *
 * @since 2.0.0
 */
@Path("/trace")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class TracesResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates a trace summary
   *
   * @param minutes
   *          Range of minutes to filter traces Min of 0 minutes, Max of 30 days
   * @return Trace summary in specified range
   */
  @Path("summary/{minutes}")
  @GET
  public RecentTracesList getTraces(
      @DefaultValue("10") @PathParam("minutes") @NotNull @Min(0) @Max(2592000) int minutes)
      throws Exception {

    return new RecentTracesList();

  }

  /**
   * Generates a list of traces filtered by type and range of minutes
   *
   * @param type
   *          Type of the trace
   * @param minutes
   *          Range of minutes, Min of 0 and Max 0f 30 days
   * @return List of traces filtered by type and range
   */
  @Path("listType/{type}/{minutes}")
  @GET
  public TraceType getTracesType(
      @PathParam("type") @NotNull @Pattern(regexp = RESOURCE_REGEX) final String type,
      @PathParam("minutes") @Min(0) @Max(2592000) int minutes) throws Exception {

    return new TraceType(type);
  }

  /**
   * Generates a list of traces filtered by ID
   *
   * @param id
   *          ID of the trace to display
   * @return traces by ID
   */
  @Path("show/{id}")
  @GET
  public TraceList getTracesType(
      @PathParam("id") @NotNull @Pattern(regexp = ALPHA_NUM_REGEX) String id) throws Exception {

    return new TraceList(id);

  }

}
