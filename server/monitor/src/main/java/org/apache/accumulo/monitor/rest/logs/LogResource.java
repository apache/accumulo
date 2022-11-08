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
package org.apache.accumulo.monitor.rest.logs;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.monitor.Monitor;

/**
 * Responsible for generating a new log JSON object
 *
 * @since 2.0.0
 */
@Path("/logs")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class LogResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates log event list as a JSON object
   *
   * @return log event array
   */
  @GET
  public List<SanitizedLogEvent> getRecentLogs() {
    return monitor.recentLogs().getSanitizedEvents();
  }

  /**
   * REST call to clear the logs
   */
  @POST
  @Path("clear")
  public void clearLogs() {
    monitor.recentLogs().clearEvents();
  }

  /**
   * REST call to append a log message
   *
   * @since 2.1.0
   */
  @POST
  @Path("append")
  @Consumes(MediaType.APPLICATION_JSON)
  public void append(SingleLogEvent event) {
    monitor.recentLogs().addEvent(event);
  }
}
