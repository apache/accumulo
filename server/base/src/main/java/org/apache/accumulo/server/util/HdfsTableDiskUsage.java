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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.NumUtil;
import org.apache.accumulo.core.util.tables.TableDiskUsage;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.base.Joiner;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class HdfsTableDiskUsage extends TableDiskUsage {

  private static final Logger log = LoggerFactory.getLogger(HdfsTableDiskUsage.class);

  public interface Printer {
    void print(String line);
  }

  public static void printDiskUsage(Collection<String> tableNames, VolumeManager fs,
      AccumuloClient client, boolean humanReadable) throws TableNotFoundException, IOException {
    printDiskUsage(tableNames, fs, client, System.out::println, humanReadable);
  }

  public static SortedMap<SortedSet<String>,Long> getDiskUsage(Set<TableId> tableIds,
      VolumeManager fs, AccumuloClient client) throws IOException {
    HdfsTableDiskUsage tdu = new HdfsTableDiskUsage();

    // Add each tableID
    for (TableId tableId : tableIds)
      tdu.addTable(tableId);

    HashSet<TableId> tablesReferenced = new HashSet<>(tableIds);
    HashSet<TableId> emptyTableIds = new HashSet<>();
    HashSet<String> nameSpacesReferenced = new HashSet<>();

    // For each table ID
    for (TableId tableId : tableIds) {
      Scanner mdScanner;
      try {
        mdScanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      mdScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

      if (!mdScanner.iterator().hasNext()) {
        emptyTableIds.add(tableId);
      }

      // Read each file referenced by that table
      for (Entry<Key,Value> entry : mdScanner) {
        String file = entry.getKey().getColumnQualifier().toString();
        String[] parts = file.split("/");
        // the filename
        String uniqueName = parts[parts.length - 1];
        if (file.contains(":") || file.startsWith("../")) {
          String ref = parts[parts.length - 3];
          // Track any tables which are referenced externally by the current table
          if (!ref.equals(tableId.canonical())) {
            tablesReferenced.add(TableId.of(ref));
          }
          if (file.contains(":") && parts.length > 3) {
            List<String> base = Arrays.asList(Arrays.copyOf(parts, parts.length - 3));
            nameSpacesReferenced.add(Joiner.on("/").join(base));
          }
        }

        // add this file to this table
        tdu.linkFileAndTable(tableId, uniqueName);
      }
    }

    // Each table seen (provided by user, or reference by table the user provided)
    for (TableId tableId : tablesReferenced) {
      for (String tableDir : nameSpacesReferenced) {
        // Find each file and add its size
        Path path = new Path(tableDir + "/" + tableId);
        if (!fs.exists(path)) {
          log.debug("Table ID directory {} does not exist.", path);
          continue;
        }
        log.info("Get all files recursively in {}", path);
        RemoteIterator<LocatedFileStatus> ri = fs.listFiles(path, true);
        while (ri.hasNext()) {
          FileStatus status = ri.next();
          String name = status.getPath().getName();
          tdu.addFileSize(name, status.getLen());
        }
      }
    }

    return buildSharedUsageMap(tdu, ((ClientContext) client), emptyTableIds);

  }

  public static void printDiskUsage(Collection<String> tableNames, VolumeManager fs,
      AccumuloClient client, Printer printer, boolean humanReadable)
      throws TableNotFoundException, IOException {

    HashSet<TableId> tableIds = new HashSet<>();

    // Get table IDs for all tables requested to be 'du'
    for (String tableName : tableNames) {
      TableId tableId = ((ClientContext) client).getTableId(tableName);
      if (tableId == null)
        throw new TableNotFoundException(null, tableName, "Table " + tableName + " not found");

      tableIds.add(tableId);
    }

    Map<SortedSet<String>,Long> usage = getDiskUsage(tableIds, fs, client);

    String valueFormat = humanReadable ? "%9s" : "%,24d";
    for (Entry<SortedSet<String>,Long> entry : usage.entrySet()) {
      Object value = humanReadable ? NumUtil.bigNumberForSize(entry.getValue()) : entry.getValue();
      printer.print(String.format(valueFormat + " %s", value, entry.getKey()));
    }
  }

  static class Opts extends ServerUtilOpts {
    @Parameter(description = " <table> { <table> ... } ")
    List<String> tables = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(HdfsTableDiskUsage.class.getName(), args);
    Span span = TraceUtil.startSpan(HdfsTableDiskUsage.class, "main");
    try (Scope scope = span.makeCurrent()) {
      try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
        VolumeManager fs = opts.getServerContext().getVolumeManager();
        HdfsTableDiskUsage.printDiskUsage(opts.tables, fs, client, false);
      } finally {
        span.end();
      }
    }
  }

}
