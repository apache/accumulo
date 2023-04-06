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
package org.apache.accumulo.monitor.rest;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response.Status;

import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.manager.ManagerResource;
import org.apache.accumulo.monitor.rest.tables.TablesResource;
import org.apache.accumulo.monitor.rest.tservers.TabletServer;

/**
 * Responsible for generating a JSON and XML summary of the Monitor
 *
 * @since 2.0.0
 */
@Path("/")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class XMLResource {

  @Inject
  private Monitor monitor;

  /**
   * Generates summary of the Monitor
   *
   * @return SummaryInformation object
   */
  public SummaryInformation getInformation() {

    ManagerMonitorInfo mmi = monitor.getMmi();
    if (mmi == null) {
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    // Add Monitor information
    SummaryInformation xml = new SummaryInformation(mmi.tServerInfo.size(),
        ManagerResource.getTables(monitor), TablesResource.getTables(monitor));

    // Add tserver information
    for (TabletServerStatus status : mmi.tServerInfo) {
      xml.addTabletServer(new TabletServer(monitor, status));
    }

    return xml;
  }

  @GET
  @Path("xml")
  @Produces(MediaType.APPLICATION_XML)
  public SummaryInformation getXMLInformation() {
    return getInformation();
  }

  @GET
  @Path("json")
  @Produces(MediaType.APPLICATION_JSON)
  public SummaryInformation getJSONInformation() {
    return getInformation();
  }
}
