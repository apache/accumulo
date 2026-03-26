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
package org.apache.accumulo.server.util.adminCommand;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class StopAll extends ServerKeywordExecutable<ServerOpts> {

  private static final Logger LOG = LoggerFactory.getLogger(StopAll.class);

  // This only exists because it is called from StandaloneClusterControl, MiniAccumuloClusterControl
  // and ITs
  public static void main(String[] args) throws Exception {
    new StopAll().execute(args);
  }

  public StopAll() {
    super(new ServerOpts());
  }

  @Override
  public String keyword() {
    return "stop-all";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.PROCESS;
  }

  @Override
  public String description() {
    return "Stop all tablet servers and the manager.";
  }

  @Override
  public void execute(JCommander cl, ServerOpts options) throws Exception {
    ServerContext context = getServerContext();
    flushAll(context);
    ThriftClientTypes.MANAGER.executeVoid(context,
        client -> client.shutdown(TraceUtil.traceInfo(), context.rpcCreds(), true));
  }

  /**
   * Flushing during shutdown is a performance optimization, it's not required. This method will
   * attempt to initiate flushes of all tables and give up if it takes too long.
   */
  private void flushAll(final ClientContext context) {

    final AtomicInteger flushesStarted = new AtomicInteger(0);

    Runnable flushTask = () -> {
      try {
        Set<String> tables = context.tableOperations().list();
        for (String table : tables) {
          if (table.equals(SystemTables.METADATA.tableName())) {
            continue;
          }
          try {
            context.tableOperations().flush(table, null, null, false);
            flushesStarted.incrementAndGet();
          } catch (TableNotFoundException e) {
            // ignore
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to initiate flush {}", e.getMessage());
      }
    };

    Thread flusher = new Thread(flushTask);
    flusher.setDaemon(true);
    flusher.start();

    long start = System.currentTimeMillis();
    try {
      flusher.join(3000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting to join Flush thread", e);
    }

    while (flusher.isAlive() && System.currentTimeMillis() - start < 15000) {
      int flushCount = flushesStarted.get();
      try {
        flusher.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while waiting to join Flush thread", e);
      }

      if (flushCount == flushesStarted.get()) {
        // no progress was made while waiting for join... maybe its stuck, stop waiting on it
        break;
      }
    }

    flusher.interrupt();
    try {
      flusher.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting to join Flush thread", e);
    }
  }

}
