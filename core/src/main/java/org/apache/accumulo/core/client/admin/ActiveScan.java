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
package org.apache.accumulo.core.client.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.TColumn;

/**
 * A class that contains information about an ActiveScan
 * 
 */

public class ActiveScan {
  
  private long scanid;
  private String client;
  private String table;
  private long age;
  private long idle;
  private ScanType type;
  private ScanState state;
  private KeyExtent extent;
  private List<Column> columns;
  private List<String> ssiList;
  private Map<String,Map<String,String>> ssio;
  private String user;
  
  ActiveScan(Instance instance, org.apache.accumulo.core.tabletserver.thrift.ActiveScan activeScan) throws TableNotFoundException {
    this.client = activeScan.client;
    this.user = activeScan.user;
    this.age = activeScan.age;
    this.idle = activeScan.idleTime;
    this.table = Tables.getTableName(instance, activeScan.tableId);
    this.type = ScanType.valueOf(activeScan.getType().name());
    this.state = ScanState.valueOf(activeScan.state.name());
    this.extent = new KeyExtent(activeScan.extent);
    
    this.columns = new ArrayList<Column>(activeScan.columns.size());
    
    for (TColumn tcolumn : activeScan.columns)
      this.columns.add(new Column(tcolumn));
    
    this.ssiList = new ArrayList<String>();
    for (IterInfo ii : activeScan.ssiList) {
      this.ssiList.add(ii.iterName + "=" + ii.priority + "," + ii.className);
    }
    this.ssio = activeScan.ssio;
  }
  
  /**
   * @return an id that uniquely identifies that scan on the server
   */
  public long getScanid() {
    return scanid;
  }
  
  /**
   * @return the address of the client that initiated the scan
   */
  
  public String getClient() {
    return client;
  }
  
  /**
   * @return the user that initiated the scan
   */
  
  public String getUser() {
    return user;
  }
  
  /**
   * @return the table the scan is running against
   */
  
  public String getTable() {
    return table;
  }
  
  /**
   * @return the age of the scan in milliseconds
   */
  
  public long getAge() {
    return age;
  }
  
  /**
   * @return milliseconds since last time client read data from the scan
   */
  
  public long getLastContactTime() {
    return idle;
  }
  
  public ScanType getType() {
    return type;
  }
  
  public ScanState getState() {
    return state;
  }
  
  /**
   * @return tablet the scan is running against, if a batch scan may be one of many or null
   */
  
  public KeyExtent getExtent() {
    return extent;
  }
  
  /**
   * @return columns requested by the scan
   */
  
  public List<Column> getColumns() {
    return columns;
  }
  
  /**
   * @return server side iterators used by the scan
   */
  
  public List<String> getSsiList() {
    return ssiList;
  }
  
  /**
   * @return server side iterator options
   */
  
  public Map<String,Map<String,String>> getSsio() {
    return ssio;
  }
  
}
