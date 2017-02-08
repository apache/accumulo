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
package org.apache.accumulo.monitor.rest.api.xml;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.master.MasterResource;
import org.apache.accumulo.monitor.rest.api.table.TablesResource;
import org.apache.accumulo.monitor.rest.api.tserver.TabletServer;

/**
 *
 * Responsible for generating an XML summary of the Monitor
 *
 * @since 2.0.0
 *
 */
@Path("/xml")
public class XMLResource {

  /**
   * Generates an XML summary of the Monitor
   *
   * @return XML summary
   */
  @GET
  @Produces(MediaType.APPLICATION_XML)
  public XMLInformation getXMLInformation() {

    MasterMonitorInfo mmi = Monitor.getMmi();
    if (null == mmi) {
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    // Add Monitor information
    XMLInformation xml = new XMLInformation(mmi.tServerInfo.size(), new MasterResource().getTables(), new TablesResource().getTables());

    // Add tserver information
    for (TabletServerStatus status : mmi.tServerInfo) {
      xml.addTablet(new TabletServer(status));
    }

    return xml;
  }
}
