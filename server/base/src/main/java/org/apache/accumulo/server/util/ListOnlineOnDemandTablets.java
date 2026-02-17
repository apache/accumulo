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

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.LiveTServerSet.Listener;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

@AutoService(KeywordExecutable.class)
public class ListOnlineOnDemandTablets extends ServerKeywordExecutable<ServerOpts> {
  private static final Logger log = LoggerFactory.getLogger(ListOnlineOnDemandTablets.class);

  public ListOnlineOnDemandTablets() {
    super(new ServerOpts());
  }

  @Override
  public String keyword() {
    return "find-online-ondemand-tablets";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.ADMIN;
  }

  @Override
  public String description() {
    return "Lists the OnDemand tablets that are currently hosted";
  }

  @Override
  public void execute(JCommander cl, ServerOpts options) throws Exception {
    Span span = TraceUtil.startSpan(ListOnlineOnDemandTablets.class, "main");
    try (Scope scope = span.makeCurrent()) {
      findOnline(getServerContext());
    } finally {
      span.end();
    }
  }

  static int findOnline(ServerContext context) throws TableNotFoundException {

    final AtomicBoolean scanning = new AtomicBoolean(false);

    LiveTServerSet tservers = new LiveTServerSet(context);

    tservers.startListeningForTabletServerChanges(new Listener() {
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
    scanning.set(true);

    System.out.println("Scanning " + SystemTables.METADATA.tableName());

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
          && tabletMetadata.getTabletAvailability() == TabletAvailability.ONDEMAND) {
        System.out.println(tabletMetadata.getExtent() + " is " + state + "  #walogs:"
            + tabletMetadata.getLogs().size());
        online++;
      }
    }

    return online;
  }
}
