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
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.LiveTServerSet.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class ListOnlineOnDemandTablets {
  private static final Logger log = LoggerFactory.getLogger(ListOnlineOnDemandTablets.class);

  public static void main(String[] args) throws Exception {
    ServerUtilOpts opts = new ServerUtilOpts();
    opts.parseArgs(ListOnlineOnDemandTablets.class.getName(), args);
    Span span = TraceUtil.startSpan(ListOnlineOnDemandTablets.class, "main");
    try (Scope scope = span.makeCurrent()) {
      ServerContext context = opts.getServerContext();
      findOnline(context);
    } finally {
      span.end();
    }
  }

  static int findOnline(ServerContext context) throws TableNotFoundException {

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

    System.out.println("Scanning " + MetadataTable.NAME);

    try (TabletsMetadata metaScanner =
        context.getAmple().readTablets().forLevel(Ample.DataLevel.USER).build()) {
      return checkTablets(context, metaScanner.iterator(), tservers);
    }
  }

  private static int checkTablets(ServerContext context, Iterator<TabletMetadata> scanner,
      LiveTServerSet tservers) {
    int online = 0;

    while (scanner.hasNext() && !System.out.checkError()) {
      TabletMetadata tabletMetadata = scanner.next();
      Set<TServerInstance> liveTServers = tservers.getCurrentServers();
      TabletState state = TabletState.compute(tabletMetadata, liveTServers);
      if (state == TabletState.HOSTED
          && tabletMetadata.getHostingGoal() == TabletHostingGoal.ONDEMAND) {
        System.out.println(tabletMetadata.getExtent() + " is " + state + "  #walogs:"
            + tabletMetadata.getLogs().size());
        online++;
      }
    }

    return online;
  }
}
