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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.rest.api.TableInformation;
import org.apache.accumulo.monitor.rest.api.TabletServerInformation;
import org.apache.accumulo.monitor.rest.api.TabletServerWithTableInformation;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
@Path("/tservers")
@Produces({MediaType.APPLICATION_JSON})
public class TabletServerResource {
  private static final Logger log = LoggerFactory.getLogger(TabletServerResource.class);

  @GET
  public List<TabletServerInformation> getTserverSummary() {
    MasterMonitorInfo mmi = Monitor.getMmi();
    if (null == mmi) {
      throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR);
    }

    ArrayList<TabletServerInformation> tserverInfo = new ArrayList<>(mmi.tServerInfo.size());
    for (TabletServerStatus status : mmi.tServerInfo) {
      tserverInfo.add(new TabletServerInformation(status));
    }

    return tserverInfo;
  }

  @Path("/{address}")
  @GET
  public Object getTserverSummary(@PathParam("address") String address, @QueryParam("tableName") String tableName) {
    MasterMonitorInfo mmi = Monitor.getMmi();
    if (null == mmi) {
      throw new WebApplicationException(Status.NOT_FOUND);
    }

    if (null != tableName) {
      return getTserverTableSummary(address, tableName);
    } else {
      for (TabletServerStatus status : mmi.tServerInfo) {
        if (address.equals(status.name)) {
          return new TabletServerInformation(status);
        }
      }
    }

    throw new WebApplicationException(Status.NOT_FOUND);
  }

  public TabletServerWithTableInformation getTserverTableSummary(String address, String tableName) {
    MasterMonitorInfo mmi = Monitor.getMmi();
    if (null == mmi) {
      log.warn("Could not fetch thrift Master information");
      throw new WebApplicationException(Status.NOT_FOUND);
    }

    String tableId = Tables.getNameToIdMap(HdfsZooInstance.getInstance()).get(tableName);
    if (null == tableId) {
      log.warn("Could not find table ID for name '{}'", tableName);
      throw new WebApplicationException(Status.NOT_FOUND);
    }

    Map<String,Double> compactingByTable = TableInfoUtil.summarizeTableStats(mmi);
    TableManager tableManager = TableManager.getInstance();

    for (TabletServerStatus status : mmi.tServerInfo) {
      if (address.equals(status.name)) {
        TableInfo thriftTableInfo = status.getTableMap().get(tableId);
        if (null != thriftTableInfo) {
          Double holdTime = compactingByTable.get(tableId);
          if (holdTime == null) {
            holdTime = new Double(0.);
          }

          TableInformation tableInfo = new TableInformation(tableName, tableId, thriftTableInfo, holdTime, tableManager.getTableState(tableId).name());
          return new TabletServerWithTableInformation(new TabletServerInformation(status), tableInfo); 
        }
      }
    }

    log.warn("Found tableId of {} for {}, but could not find table hosted on", tableId, tableName, address);
    throw new WebApplicationException(Status.NOT_FOUND);
  }
}
