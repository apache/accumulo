/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.hadoop.io.Text;
import org.apache.htrace.TraceScope;

public class CheckForMetadataProblems {
  private static boolean sawProblems = false;

  private static void checkTable(TableId tableId, TreeSet<KeyExtent> tablets) {
    // sanity check of metadata table entries
    // make sure tablets has no holes, and that it starts and ends w/ null

    if (tablets.isEmpty()) {
      System.out.println("No entries found in metadata table for table " + tableId);
      sawProblems = true;
      return;
    }

    if (tablets.first().prevEndRow() != null) {
      System.out.println("First entry for table " + tableId + "- " + tablets.first()
          + " - has non null prev end row");
      sawProblems = true;
      return;
    }

    if (tablets.last().endRow() != null) {
      System.out.println(
          "Last entry for table " + tableId + "- " + tablets.last() + " - has non null end row");
      sawProblems = true;
      return;
    }

    Iterator<KeyExtent> tabIter = tablets.iterator();
    Text lastEndRow = tabIter.next().endRow();
    boolean everythingLooksGood = true;
    while (tabIter.hasNext()) {
      KeyExtent tabke = tabIter.next();
      boolean broke = false;
      if (tabke.prevEndRow() == null) {
        System.out
            .println("Table " + tableId + " has null prev end row in middle of table " + tabke);
        broke = true;
      } else if (!tabke.prevEndRow().equals(lastEndRow)) {
        System.out.println(
            "Table " + tableId + " has a hole " + tabke.prevEndRow() + " != " + lastEndRow);
        broke = true;
      }
      if (broke) {
        everythingLooksGood = false;
      }

      lastEndRow = tabke.endRow();
    }
    if (everythingLooksGood)
      System.out.println("All is well for table " + tableId);
    else
      sawProblems = true;
  }

  private static void checkMetadataAndRootTableEntries(String tableNameToCheck, ServerUtilOpts opts)
      throws Exception {
    System.out.println("Checking table: " + tableNameToCheck);
    Map<TableId,TreeSet<KeyExtent>> tables = new HashMap<>();

    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {

      Scanner scanner = client.createScanner(tableNameToCheck, Authorizations.EMPTY);

      scanner.setRange(TabletsSection.getRange());
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
      scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);

      Text colf = new Text();
      Text colq = new Text();
      boolean justLoc = false;

      int count = 0;

      for (Entry<Key,Value> entry : scanner) {
        colf = entry.getKey().getColumnFamily(colf);
        colq = entry.getKey().getColumnQualifier(colq);

        count++;

        TableId tableId = KeyExtent.fromMetaRow(entry.getKey().getRow()).tableId();

        TreeSet<KeyExtent> tablets = tables.get(tableId);
        if (tablets == null) {

          tables.forEach(CheckForMetadataProblems::checkTable);

          tables.clear();

          tablets = new TreeSet<>();
          tables.put(tableId, tablets);
        }

        if (TabletColumnFamily.PREV_ROW_COLUMN.equals(colf, colq)) {
          KeyExtent tabletKe = KeyExtent.fromMetaPrevRow(entry);
          tablets.add(tabletKe);
          justLoc = false;
        } else if (colf.equals(CurrentLocationColumnFamily.NAME)) {
          if (justLoc) {
            System.out.println("Problem at key " + entry.getKey());
            sawProblems = true;
          }
          justLoc = true;
        }
      }

      if (count == 0) {
        System.err.println("ERROR : " + tableNameToCheck + " table is empty");
        sawProblems = true;
      }
    }

    tables.forEach(CheckForMetadataProblems::checkTable);

    if (!sawProblems) {
      System.out.println("No problems found");
    }
    // end METADATA table sanity check
  }

  public static void main(String[] args) throws Exception {
    ServerUtilOpts opts = new ServerUtilOpts();
    try (TraceScope clientSpan =
        opts.parseArgsAndTrace(CheckForMetadataProblems.class.getName(), args)) {

      checkMetadataAndRootTableEntries(RootTable.NAME, opts);
      checkMetadataAndRootTableEntries(MetadataTable.NAME, opts);
      if (sawProblems)
        throw new RuntimeException();
    }
  }

}
