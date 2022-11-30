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
package org.apache.accumulo.server.util;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.LiveTServerSet.Listener;
import org.apache.accumulo.server.manager.state.MetaDataTableScanner;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class FindOfflineTablets {
  private static final Logger log = LoggerFactory.getLogger(FindOfflineTablets.class);

  public static void main(String[] args) throws Exception {
    ServerUtilOpts opts = new ServerUtilOpts();
    opts.parseArgs(FindOfflineTablets.class.getName(), args);
    Span span = TraceUtil.startSpan(FindOfflineTablets.class, "main");
    try (Scope scope = span.makeCurrent()) {
      ServerContext context = opts.getServerContext();
      findOffline(context, null);
    } finally {
      span.end();
    }
  }

  static int findOffline(ServerContext context, String tableName) throws TableNotFoundException {

    final AtomicBoolean scanning = new AtomicBoolean(false);

    LiveTServerSet tservers = new LiveTServerSet(context, new Listener() {
      @Override
      public void update(LiveTServerSet current, Set<TServerInstance> deleted,
          Set<TServerInstance> added) {
        if (!deleted.isEmpty() && scanning.get()) {
          log.warn("Tablet servers deleted while scanning: {}", deleted);
        }
        if (!added.isEmpty() && scanning.get()) {
          log.warn("Tablet servers added while scanning: {}", added);
        }
      }
    });
    tservers.startListeningForTabletServerChanges();
    scanning.set(true);

    Iterator<TabletLocationState> zooScanner =
        TabletStateStore.getStoreForLevel(DataLevel.ROOT, context).iterator();

    int offline = 0;

    System.out.println("Scanning zookeeper");
    if ((offline = checkTablets(context, zooScanner, tservers)) > 0) {
      return offline;
    }

    if (RootTable.NAME.equals(tableName)) {
      return 0;
    }

    System.out.println("Scanning " + RootTable.NAME);
    Iterator<TabletLocationState> rootScanner =
        new MetaDataTableScanner(context, TabletsSection.getRange(), RootTable.NAME);
    if ((offline = checkTablets(context, rootScanner, tservers)) > 0) {
      return offline;
    }

    if (MetadataTable.NAME.equals(tableName)) {
      return 0;
    }

    System.out.println("Scanning " + MetadataTable.NAME);

    Range range = TabletsSection.getRange();
    if (tableName != null) {
      TableId tableId = context.getTableId(tableName);
      range = new KeyExtent(tableId, null, null).toMetaRange();
    }

    try (MetaDataTableScanner metaScanner =
        new MetaDataTableScanner(context, range, MetadataTable.NAME)) {
      return checkTablets(context, metaScanner, tservers);
    }
  }

  private static int checkTablets(ServerContext context, Iterator<TabletLocationState> scanner,
      LiveTServerSet tservers) {
    int offline = 0;

    while (scanner.hasNext() && !System.out.checkError()) {
      TabletLocationState locationState = scanner.next();
      TabletState state = locationState.getState(tservers.getCurrentServers());
      if (state != null && state != TabletState.HOSTED
          && context.getTableManager().getTableState(locationState.extent.tableId())
              != TableState.OFFLINE) {
        System.out
            .println(locationState + " is " + state + "  #walogs:" + locationState.walogs.size());
        offline++;
      }
    }

    return offline;
  }
}
