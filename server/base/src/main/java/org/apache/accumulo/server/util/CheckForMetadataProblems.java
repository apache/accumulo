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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.JCommander;
import com.google.auto.service.AutoService;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

@AutoService(KeywordExecutable.class)
public class CheckForMetadataProblems extends ServerKeywordExecutable<ServerOpts> {

  public CheckForMetadataProblems() {
    super(new ServerOpts());
  }

  @Override
  public String keyword() {
    return "check-metadata";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.INSTANCE;
  }

  @Override
  public String description() {
    return "Checks root and metadata table entries for problems";
  }

  private static boolean checkTable(ServerContext context, TableId tableId,
      TreeSet<KeyExtent> tablets, ServerOpts opts, Consumer<String> printInfoMethod,
      Consumer<String> printProblemMethod) {
    // sanity check of metadata table entries
    // make sure tablets have no holes, and that it starts and ends w/ null
    String tableName;
    boolean sawProblems = false;

    try {
      tableName = context.getQualifiedTableName(tableId);
    } catch (TableNotFoundException e) {
      tableName = null;
    }

    printInfoMethod.accept(String.format("Ensuring tablets for table %s (%s) have: no holes, "
        + "valid (null) prev end row for first tablet, and valid (null) end row "
        + "for last tablet...\n", tableName, tableId));

    if (tablets.isEmpty()) {
      printProblemMethod.accept(String
          .format("...No entries found in metadata table for table %s (%s)", tableName, tableId));
      return true;
    }

    if (tablets.first().prevEndRow() != null) {
      printProblemMethod
          .accept(String.format("...First entry for table %s (%s) - %s - has non-null prev end row",
              tableName, tableId, tablets.first()));
      return true;
    }

    if (tablets.last().endRow() != null) {
      printProblemMethod
          .accept(String.format("...Last entry for table %s (%s) - %s - has non-null end row",
              tableName, tableId, tablets.last()));
      return true;
    }

    Iterator<KeyExtent> tabIter = tablets.iterator();
    Text lastEndRow = tabIter.next().endRow();
    boolean everythingLooksGood = true;
    while (tabIter.hasNext()) {
      KeyExtent table = tabIter.next();
      boolean broke = false;
      if (table.prevEndRow() == null) {
        printProblemMethod
            .accept(String.format("...Table %s (%s) has null prev end row in middle of table %s",
                tableName, tableId, table));
        broke = true;
      } else if (!table.prevEndRow().equals(lastEndRow)) {
        printProblemMethod.accept(String.format("...Table %s (%s) has a hole %s != %s", tableName,
            tableId, table.prevEndRow(), lastEndRow));
        broke = true;
      }
      if (broke) {
        everythingLooksGood = false;
      }

      lastEndRow = table.endRow();
    }
    if (everythingLooksGood) {
      printInfoMethod.accept(String.format("...All is well for table %s (%s)", tableName, tableId));
    } else {
      sawProblems = true;
    }

    return sawProblems;
  }

  public static boolean checkMetadataAndRootTableEntries(ServerContext context,
      String tableNameToCheck, ServerOpts opts, Consumer<String> printInfoMethod,
      Consumer<String> printProblemMethod) throws Exception {
    TableId tableCheckId = context.getTableId(tableNameToCheck);
    printInfoMethod.accept(String.format("Checking tables whose metadata is found in: %s (%s)...\n",
        tableNameToCheck, tableCheckId));
    Map<TableId,TreeSet<KeyExtent>> tables = new HashMap<>();
    boolean sawProblems = false;

    try (Scanner scanner = context.createScanner(tableNameToCheck, Authorizations.EMPTY)) {

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

          for (var e : tables.entrySet()) {
            sawProblems = CheckForMetadataProblems.checkTable(context, e.getKey(), e.getValue(),
                opts, printInfoMethod, printProblemMethod) || sawProblems;
          }

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
            printProblemMethod.accept("Problem at key " + entry.getKey());
            sawProblems = true;
          }
          justLoc = true;
        }
      }

      if (count == 0) {
        printProblemMethod.accept(
            String.format("ERROR : table %s (%s) is empty", tableNameToCheck, tableCheckId));
        sawProblems = true;
      }
    }

    for (var e : tables.entrySet()) {
      sawProblems = CheckForMetadataProblems.checkTable(context, e.getKey(), e.getValue(), opts,
          printInfoMethod, printProblemMethod) || sawProblems;
    }

    if (!sawProblems) {
      printInfoMethod.accept(
          String.format("\n...No problems found in %s (%s)", tableNameToCheck, tableCheckId));
    }
    // end METADATA table sanity check
    return sawProblems;
  }

  @Override
  public void execute(JCommander cl, ServerOpts options) throws Exception {
    Span span = TraceUtil.startSpan(CheckForMetadataProblems.class, "main");
    boolean sawProblems;
    try (Scope scope = span.makeCurrent()) {

      sawProblems = checkMetadataAndRootTableEntries(getServerContext(),
          SystemTables.ROOT.tableName(), options, System.out::println, System.out::println);
      System.out.println();
      sawProblems =
          checkMetadataAndRootTableEntries(getServerContext(), SystemTables.METADATA.tableName(),
              options, System.out::println, System.out::println) || sawProblems;
      if (sawProblems) {
        throw new IllegalStateException();
      }
    } catch (Exception e) {
      TraceUtil.setException(span, e, true);
      throw e;
    } finally {
      span.end();
    }
  }

  // This is called from SplitIT
  public static void main(String[] args) throws Exception {
    CheckForMetadataProblems check = new CheckForMetadataProblems();
    check.execute(args);
  }
}
