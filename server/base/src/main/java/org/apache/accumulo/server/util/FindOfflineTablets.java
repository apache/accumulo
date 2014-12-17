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
package org.apache.accumulo.server.util;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.server.cli.ClientOpts;
import org.apache.accumulo.server.master.LiveTServerSet;
import org.apache.accumulo.server.master.LiveTServerSet.Listener;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class FindOfflineTablets {
  private static final Logger log = Logger.getLogger(FindOfflineTablets.class);

  public static void main(String[] args) throws Exception {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(FindOfflineTablets.class.getName(), args);
    Instance instance = opts.getInstance();
    SystemCredentials creds = SystemCredentials.get();

    findOffline(instance, creds, null);
  }

  static int findOffline(Instance instance, Credentials creds, String tableName) throws AccumuloException, TableNotFoundException {

    final AtomicBoolean scanning = new AtomicBoolean(false);

    LiveTServerSet tservers = new LiveTServerSet(instance, DefaultConfiguration.getDefaultConfiguration(), new Listener() {
      @Override
      public void update(LiveTServerSet current, Set<TServerInstance> deleted, Set<TServerInstance> added) {
        if (!deleted.isEmpty() && scanning.get())
          log.warn("Tablet servers deleted while scanning: " + deleted);
        if (!added.isEmpty() && scanning.get())
          log.warn("Tablet servers added while scanning: " + added);
      }
    });
    tservers.startListeningForTabletServerChanges();
    scanning.set(true);

    Iterator<TabletLocationState> zooScanner;
    try {
      zooScanner = new ZooTabletStateStore().iterator();
    } catch (DistributedStoreException e) {
      throw new AccumuloException(e);
    }

    int offline = 0;

    System.out.println("Scanning zookeeper");
    if ((offline = checkTablets(zooScanner, tservers)) > 0)
      return offline;

    if (RootTable.NAME.equals(tableName))
      return 0;

    System.out.println("Scanning " + RootTable.NAME);
    Iterator<TabletLocationState> rootScanner = new MetaDataTableScanner(instance, creds, MetadataSchema.TabletsSection.getRange(), RootTable.NAME);
    if ((offline = checkTablets(rootScanner, tservers)) > 0)
      return offline;

    if (MetadataTable.NAME.equals(tableName))
      return 0;

    System.out.println("Scanning " + MetadataTable.NAME);

    Range range = MetadataSchema.TabletsSection.getRange();
    if (tableName != null) {
      String tableId = Tables.getTableId(instance, tableName);
      range = new KeyExtent(new Text(tableId), null, null).toMetadataRange();
    }

    MetaDataTableScanner metaScanner = new MetaDataTableScanner(instance, creds, range, MetadataTable.NAME);
    try {
      return checkTablets(metaScanner, tservers);
    } finally {
      metaScanner.close();
    }
  }

  private static int checkTablets(Iterator<TabletLocationState> scanner, LiveTServerSet tservers) {
    int offline = 0;

    while (scanner.hasNext() && !System.out.checkError()) {
      TabletLocationState locationState = scanner.next();
      TabletState state = locationState.getState(tservers.getCurrentServers());
      if (state != null && state != TabletState.HOSTED
          && TableManager.getInstance().getTableState(locationState.extent.getTableId().toString()) != TableState.OFFLINE) {
        System.out.println(locationState + " is " + state + "  #walogs:" + locationState.walogs.size());
        offline++;
      }
    }

    return offline;
  }
}
