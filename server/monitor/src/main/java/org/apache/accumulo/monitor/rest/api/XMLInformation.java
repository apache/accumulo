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
package org.apache.accumulo.monitor.rest.api;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name="stats")
public class XMLInformation {

  public List<TabletServer> servers;
  
  public String masterGoalState, masterState;
  
  public BadTabletServers badTabletServers; 
  public ServersShuttingDown tabletServersShuttingDown;
  public Integer unassignedTablets;
  public DeadServerList deadTabletServers;

  public DeadLoggerList deadLoggers;  
  
  public TableInformationList tables;
  
  public Totals totals;

  public XMLInformation() {
    servers = new ArrayList<>();
  }

  public XMLInformation(int size, MasterInformation info, TablesList tablesList) {
    this.servers = new ArrayList<>(size);
    
    this.masterGoalState = info.masterGoalState;
    this.masterState = info.masterState;
    
    this.badTabletServers = info.badTabletServers;
    this.tabletServersShuttingDown = info.tabletServersShuttingDown;
    this.unassignedTablets = info.unassignedTablets;
    this.deadTabletServers = info.deadTabletServers;
    this.deadLoggers = info.deadLoggers;
    
    getTableInformationList(tablesList.tables);
    
    this.totals = new Totals(info.ingestrate, info.queryrate, info.numentries);
  }

  public void addTablet(TabletServer tablet) {
    servers.add(tablet);
  }
  
  /**
   * For backwards compatibility, gets all the tables without namespace information
   * @param namespaces
   */
  private void getTableInformationList(List<TableNamespace> namespaces) {
       
    this.tables = new TableInformationList();
    
    for (TableNamespace namespace : namespaces) {
      for (TableInformation info : namespace.table) {
        tables.addTable(info);
      }
    }
    
  }
}
