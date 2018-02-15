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
package org.apache.accumulo.monitor.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.master.MasterResource;
import org.apache.accumulo.monitor.rest.tables.TablesResource;
import org.apache.accumulo.monitor.rest.tservers.TabletServer;

/**
 *
 * Responsible for generating a JSON and XML summary of the Monitor
 *
 * @since 2.0.0
 *
 */
@Path("/")
@Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
public class XMLResource {

  /**
   * Generates summary of the Monitor
   *
   * @return SummaryInformation object
   */
  public SummaryInformation getInformation() {

    MasterMonitorInfo mmi = Monitor.getMmi();
    if (null == mmi) {
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    // Add Monitor information
    SummaryInformation xml = new SummaryInformation(mmi.tServerInfo.size(), MasterResource.getTables(), TablesResource.getTables());

    // Add tserver information
    for (TabletServerStatus status : mmi.tServerInfo) {
      xml.addTabletServer(new TabletServer(status));
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
