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

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.hadoop.fs.Path;

public class ListVolumesUsed {

  public static void main(String[] args) throws Exception {
    Instance instance = HdfsZooInstance.getInstance();
    listVolumes(new AccumuloServerContext(instance, new ServerConfigurationFactory(instance)));
  }

  private static String getTableURI(String rootTabletDir) {
    Path ret = FileType.TABLE.getVolume(new Path(rootTabletDir));
    if (ret == null)
      return "RELATIVE";
    return ret.toString();
  }

  private static String getLogURI(String logEntry) {
    Path ret = FileType.WAL.getVolume(new Path(logEntry));
    if (ret == null)
      return "RELATIVE";
    return ret.toString();
  }

  private static void getLogURIs(TreeSet<String> volumes, LogEntry logEntry) {
    volumes.add(getLogURI(logEntry.filename));
  }

  private static void listZookeeper() throws Exception {
    System.out.println("Listing volumes referenced in zookeeper");
    TreeSet<String> volumes = new TreeSet<>();

    volumes.add(getTableURI(MetadataTableUtil.getRootTabletDir()));
    ArrayList<LogEntry> result = new ArrayList<>();
    MetadataTableUtil.getRootLogEntries(result);
    for (LogEntry logEntry : result) {
      getLogURIs(volumes, logEntry);
    }

    for (String volume : volumes)
      System.out.println("\tVolume : " + volume);

  }

  private static void listTable(String name, Connector conn) throws Exception {

    System.out.println("Listing volumes referenced in " + name + " tablets section");

    Scanner scanner = conn.createScanner(name, Authorizations.EMPTY);

    scanner.setRange(MetadataSchema.TabletsSection.getRange());
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LogColumnFamily.NAME);
    MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);

    TreeSet<String> volumes = new TreeSet<>();

    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
        volumes.add(getTableURI(entry.getKey().getColumnQualifier().toString()));
      } else if (entry.getKey().getColumnFamily().equals(MetadataSchema.TabletsSection.LogColumnFamily.NAME)) {
        LogEntry le = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
        getLogURIs(volumes, le);
      } else if (MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(entry.getKey())) {
        volumes.add(getTableURI(entry.getValue().toString()));
      }
    }

    for (String volume : volumes)
      System.out.println("\tVolume : " + volume);

    volumes.clear();

    scanner.clearColumns();
    scanner.setRange(MetadataSchema.DeletesSection.getRange());

    for (Entry<Key,Value> entry : scanner) {
      String delPath = entry.getKey().getRow().toString().substring(MetadataSchema.DeletesSection.getRowPrefix().length());
      volumes.add(getTableURI(delPath));
    }

    System.out.println("Listing volumes referenced in " + name + " deletes section (volume replacement occurrs at deletion time)");

    for (String volume : volumes)
      System.out.println("\tVolume : " + volume);

    volumes.clear();

    WalStateManager wals = new WalStateManager(conn.getInstance(), ZooReaderWriter.getInstance());
    for (Path path : wals.getAllState().keySet()) {
      volumes.add(getLogURI(path.toString()));
    }

    System.out.println("Listing volumes referenced in " + name + " current logs");

    for (String volume : volumes)
      System.out.println("\tVolume : " + volume);
  }

  public static void listVolumes(ClientContext context) throws Exception {
    Connector conn = context.getConnector();
    listZookeeper();
    System.out.println();
    listTable(RootTable.NAME, conn);
    System.out.println();
    listTable(MetadataTable.NAME, conn);
  }

}
