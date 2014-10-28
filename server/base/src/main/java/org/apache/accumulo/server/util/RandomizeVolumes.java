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

import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.log4j.Logger;

public class RandomizeVolumes {
  private static final Logger log = Logger.getLogger(RandomizeVolumes.class);
  
  public static void main(String[] args) {
    ClientOnRequiredTable opts = new ClientOnRequiredTable();
    opts.parseArgs(RandomizeVolumes.class.getName(), args);
    String principal = SystemCredentials.get().getPrincipal();
    AuthenticationToken token = SystemCredentials.get().getToken();
    try {
      int status = randomize(opts.getInstance(), new Credentials(principal, token), opts.getTableName());
      System.exit(status);
    } catch (Exception ex) {
      log.error(ex, ex);
      System.exit(4);
    }
  }

  public static int randomize(Instance instance, Credentials credentials, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    SiteConfiguration siteConfiguration = new ServerConfigurationFactory(instance).getSiteConfiguration();
    String volumesConfig = siteConfiguration.get(Property.INSTANCE_VOLUMES);
    if (null == volumesConfig || "".equals(volumesConfig)) {
      log.error(Property.INSTANCE_VOLUMES.getKey() + " is not set in accumulo-site.xml");
      return 1;
    }
    String[] volumes = volumesConfig.split(",");
    if (volumes.length < 2) {
      log.error("There are not enough volumes configured: " + volumesConfig);
      return 2;
    }
    Connector c = instance.getConnector(credentials.getPrincipal(), credentials.getToken());
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    if (null == tableId) {
      log.error("Could not determine the table ID for table " + tableName);
      return 3;
    }
    TableState tableState = TableManager.getInstance().getTableState(tableId);
    if (TableState.OFFLINE != tableState) {
      log.debug("Taking " + tableName + " offline");
      c.tableOperations().offline(tableName, true);
      log.debug(tableName + " offline");
    }
    log.debug("Rewriting entries for " + tableName);
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    final ColumnFQ DIRCOL = MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
    DIRCOL.fetch(scanner);
    scanner.setRange(MetadataSchema.TabletsSection.getRange(tableId));
    BatchWriter writer = c.createBatchWriter(MetadataTable.NAME, null);
    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      String oldLocation = entry.getValue().toString();
      String directory;
      if (oldLocation.contains(":")) {
        String[] parts = oldLocation.split("/");
        String tableIdEntry = parts[parts.length - 2];
        if (!tableIdEntry.equals(tableId)) {
          log.error("Unexpected table id found: " + tableIdEntry + ", expected " + tableId + "; skipping");
          continue;
        }
        directory = parts[parts.length - 1];
      } else {
        directory = oldLocation.substring(1);
      }
      Mutation m = new Mutation(entry.getKey().getRow());
      String newLocation = volumes[count % volumes.length] + "/" + ServerConstants.TABLE_DIR + "/" + tableId + "/" + directory;
      m.put(DIRCOL.getColumnFamily(), DIRCOL.getColumnQualifier(), new Value(newLocation.getBytes()));
      if (log.isTraceEnabled()) {
        log.trace("Replacing " + oldLocation + " with " + newLocation);
      }
      writer.addMutation(m);
      count++;
    }
    writer.close();
    log.info("Updated " + count + " entries for table " + tableName);
    if (TableState.OFFLINE != tableState) {
      c.tableOperations().online(tableName, true);
      log.info("table " + tableName + " back online");
    }
    return 0;
  }
  
}
