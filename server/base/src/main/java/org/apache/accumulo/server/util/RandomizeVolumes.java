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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class RandomizeVolumes {
  private static final Logger log = LoggerFactory.getLogger(RandomizeVolumes.class);

  static class RandomizeOpts extends ServerUtilOpts {
    @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
    String tableName;
  }

  public static void main(String[] args) {
    RandomizeOpts opts = new RandomizeOpts();
    try (TraceScope clientSpan = opts.parseArgsAndTrace(RandomizeVolumes.class.getName(), args)) {
      ServerContext context = opts.getServerContext();
      try {
        int status = randomize(context, opts.tableName);
        System.exit(status);
      } catch (Exception ex) {
        log.error("{}", ex.getMessage(), ex);
        System.exit(4);
      }
    }
  }

  public static int randomize(ServerContext context, String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    final VolumeManager vm = context.getVolumeManager();
    if (vm.getVolumes().size() < 2) {
      log.error("There are not enough volumes configured");
      return 1;
    }
    String tblStr = context.tableOperations().tableIdMap().get(tableName);
    if (tblStr == null) {
      log.error("Could not determine the table ID for table {}", tableName);
      return 2;
    }
    TableId tableId = TableId.of(tblStr);
    TableState tableState = context.getTableManager().getTableState(tableId);
    if (tableState != TableState.OFFLINE) {
      log.info("Taking {} offline", tableName);
      context.tableOperations().offline(tableName, true);
      log.info("{} offline", tableName);
    }
    SimpleThreadPool pool = new SimpleThreadPool(50, "directory maker");
    log.info("Rewriting entries for {}", tableName);
    Scanner scanner = context.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    DIRECTORY_COLUMN.fetch(scanner);
    scanner.setRange(TabletsSection.getRange(tableId));
    BatchWriter writer = context.createBatchWriter(MetadataTable.NAME, null);
    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      String oldLocation = entry.getValue().toString();
      String directory;
      if (oldLocation.contains(":")) {
        String[] parts = oldLocation.split(Path.SEPARATOR);
        TableId tableIdEntry = TableId.of(parts[parts.length - 2]);
        if (!tableIdEntry.equals(tableId)) {
          log.error("Unexpected table id found: {}, expected {}; skipping", tableIdEntry, tableId);
          continue;
        }
        directory = parts[parts.length - 1];
      } else {
        directory = oldLocation.substring(Path.SEPARATOR.length());
      }
      Key key = entry.getKey();
      Mutation m = new Mutation(key.getRow());

      VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironmentImpl(tableId,
          new KeyExtent(key.getRow(), (Text) null).getEndRow(), context);
      final String newLocation =
          vm.choose(chooserEnv, ServerConstants.getBaseUris(context)) + Path.SEPARATOR
              + ServerConstants.TABLE_DIR + Path.SEPARATOR + tableId + Path.SEPARATOR + directory;
      m.put(key.getColumnFamily(), key.getColumnQualifier(),
          new Value(newLocation.getBytes(UTF_8)));
      if (log.isTraceEnabled()) {
        log.trace("Replacing {} with {}", oldLocation, newLocation);
      }
      writer.addMutation(m);
      pool.submit(() -> {
        try {
          vm.mkdirs(new Path(newLocation));
        } catch (IOException ex) {
          // nevermind
        }
      });
      count++;
    }
    writer.close();
    pool.shutdown();
    while (!pool.isTerminated()) {
      log.trace("Waiting for mkdir() calls to finish");
      try {
        pool.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    log.info("Updated {} entries for table {}", count, tableName);
    if (tableState != TableState.OFFLINE) {
      context.tableOperations().online(tableName, true);
      log.info("table {} back online", tableName);
    }
    return 0;
  }

}
