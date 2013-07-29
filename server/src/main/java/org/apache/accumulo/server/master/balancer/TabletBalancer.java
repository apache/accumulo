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
package org.apache.accumulo.server.master.balancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public abstract class TabletBalancer {
  
  private static final Logger log = Logger.getLogger(TabletBalancer.class);
  
  protected ServerConfiguration configuration;
  
  /**
   * Initialize the TabletBalancer. This gives the balancer the opportunity to read the configuration.
   */
  public void init(ServerConfiguration conf) {
    configuration = conf;
  }
  
  /**
   * Assign tablets to tablet servers. This method is called whenever the master finds tablets that are unassigned.
   * 
   * @param current
   *          The current table-summary state of all the online tablet servers. Read-only. The TabletServerStatus for each server may be null if the tablet
   *          server has not yet responded to a recent request for status.
   * @param unassigned
   *          A map from unassigned tablet to the last known tablet server. Read-only.
   * @param assignments
   *          A map from tablet to assigned server. Write-only.
   */
  abstract public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
      Map<KeyExtent,TServerInstance> assignments);
  
  /**
   * Ask the balancer if any migrations are necessary.
   * 
   * @param current
   *          The current table-summary state of all the online tablet servers. Read-only.
   * @param migrations
   *          the current set of migrations. Read-only.
   * @param migrationsOut
   *          new migrations to perform; should not contain tablets in the current set of migrations. Write-only.
   * @return the time, in milliseconds, to wait before re-balancing.
   * 
   *         This method will not be called when there are unassigned tablets.
   */
  public abstract long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut);
  
  /**
   * Fetch the tablets for the given table by asking the tablet server. Useful if your balance strategy needs details at the tablet level to decide what tablets
   * to move.
   * 
   * @param tserver
   *          The tablet server to ask.
   * @param tableId
   *          The table id
   * @return a list of tablet statistics
   * @throws ThriftSecurityException
   *           tablet server disapproves of your internal System password.
   * @throws TException
   *           any other problem
   */
  public List<TabletStats> getOnlineTabletsForTable(TServerInstance tserver, String tableId) throws ThriftSecurityException, TException {
    log.debug("Scanning tablet server " + tserver + " for table " + tableId);
    Client client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), tserver.getLocation(), configuration.getConfiguration());
    try {
      List<TabletStats> onlineTabletsForTable = client.getTabletStats(Tracer.traceInfo(), SystemCredentials.get().toThrift(configuration.getInstance()),
          tableId);
      return onlineTabletsForTable;
    } catch (TTransportException e) {
      log.error("Unable to connect to " + tserver + ": " + e);
    } finally {
      ThriftUtil.returnClient(client);
    }
    return null;
  }
  
  /**
   * Utility to ensure that the migrations from balance() are consistent:
   * <ul>
   * <li>Tablet objects are not null
   * <li>Source and destination tablet servers are not null and current
   * </ul>
   * 
   * @param current
   * @param migrations
   * @return A list of TabletMigration object that passed sanity checks.
   */
  public static List<TabletMigration> checkMigrationSanity(Set<TServerInstance> current, List<TabletMigration> migrations) {
    List<TabletMigration> result = new ArrayList<TabletMigration>(migrations.size());
    for (TabletMigration m : migrations) {
      if (m.tablet == null) {
        log.warn("Balancer gave back a null tablet " + m);
        continue;
      }
      if (m.newServer == null) {
        log.warn("Balancer did not set the destination " + m);
        continue;
      }
      if (m.oldServer == null) {
        log.warn("Balancer did not set the source " + m);
        continue;
      }
      if (!current.contains(m.oldServer)) {
        log.warn("Balancer wants to move a tablet from a server that is not current: " + m);
        continue;
      }
      if (!current.contains(m.newServer)) {
        log.warn("Balancer wants to move a tablet to a server that is not current: " + m);
        continue;
      }
      result.add(m);
    }
    return result;
  }
  
}
