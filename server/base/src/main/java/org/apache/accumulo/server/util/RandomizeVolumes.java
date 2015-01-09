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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class RandomizeVolumes {
  private static final Logger log = Logger.getLogger(RandomizeVolumes.class);

  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException {
    ClientOnRequiredTable opts = new ClientOnRequiredTable();
    opts.parseArgs(RandomizeVolumes.class.getName(), args);
    Connector c;
    if (opts.getToken() == null) {
      SystemCredentials creds = SystemCredentials.get();
      c = opts.getInstance().getConnector(creds.getPrincipal(), creds.getToken());
    } else {
      c = opts.getConnector();
    }
    try {
      int status = randomize(c, opts.tableName);
      System.exit(status);
    } catch (Exception ex) {
      log.error(ex, ex);
      System.exit(4);
    }
  }

  public static int randomize(Connector c, String tableName) throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
    final VolumeManager vm = VolumeManagerImpl.get();
    if (vm.getVolumes().size() < 2) {
      log.error("There are not enough volumes configured");
      return 1;
    }
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    if (null == tableId) {
      log.error("Could not determine the table ID for table " + tableName);
      return 2;
    }
    TableState tableState = TableManager.getInstance().getTableState(tableId);
    if (TableState.OFFLINE != tableState) {
      log.info("Taking " + tableName + " offline");
      c.tableOperations().offline(tableName, true);
      log.info(tableName + " offline");
    }
    SimpleThreadPool pool = new SimpleThreadPool(50, "directory maker");
    log.info("Rewriting entries for " + tableName);
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    DIRECTORY_COLUMN.fetch(scanner);
    scanner.setRange(TabletsSection.getRange(tableId));
    BatchWriter writer = c.createBatchWriter(MetadataTable.NAME, null);
    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      String oldLocation = entry.getValue().toString();
      String directory;
      if (oldLocation.contains(":")) {
        String[] parts = oldLocation.split(Path.SEPARATOR);
        String tableIdEntry = parts[parts.length - 2];
        if (!tableIdEntry.equals(tableId)) {
          log.error("Unexpected table id found: " + tableIdEntry + ", expected " + tableId + "; skipping");
          continue;
        }
        directory = parts[parts.length - 1];
      } else {
        directory = oldLocation.substring(Path.SEPARATOR.length());
      }
      Key key = entry.getKey();
      Mutation m = new Mutation(key.getRow());

      final String newLocation = vm.choose(ServerConstants.getBaseUris()) + Path.SEPARATOR + ServerConstants.TABLE_DIR + Path.SEPARATOR + tableId
          + Path.SEPARATOR + directory;
      m.put(key.getColumnFamily(), key.getColumnQualifier(), new Value(newLocation.getBytes(UTF_8)));
      if (log.isTraceEnabled()) {
        log.trace("Replacing " + oldLocation + " with " + newLocation);
      }
      writer.addMutation(m);
      pool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            vm.mkdirs(new Path(newLocation));
          } catch (IOException ex) {
            // nevermind
          }
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
    log.info("Updated " + count + " entries for table " + tableName);
    if (TableState.OFFLINE != tableState) {
      c.tableOperations().online(tableName, true);
      log.info("table " + tableName + " back online");
    }
    return 0;
  }

}
