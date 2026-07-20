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
package org.apache.accumulo.server.util.checkCommand;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.constraints.SystemEnvironment;
import org.apache.hadoop.io.Text;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public interface MetadataCheckRunner extends CheckRunner {

  String tableName();

  TableId tableId();

  default Set<ColumnFQ> requiredColFQs() {
    return Set.of(MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN);
  }

  default Set<Text> requiredColFams() {
    return Set.of(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
  }

  default String scanning() {
    return String.format("%s (%s) table", tableName(), tableId());
  }

  /**
   * Ensures that the {@link #tableName()} table (either metadata or root table) has all columns
   * that are expected. For the root metadata, ensures that the expected "columns" exist in ZK.
   */
  default boolean checkRequiredColumns(ServerContext context) throws Exception {
    Set<ColumnFQ> requiredColFQs;
    Set<Text> requiredColFams;
    boolean missingReqCol = false;

    log.trace("Scanning the {} for missing required columns...\n", scanning());
    try (Scanner scanner = context.createScanner(tableName(), Authorizations.EMPTY)) {
      var is = new IteratorSetting(100, "tablets", WholeRowIterator.class);
      scanner.addScanIterator(is);
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
      fetchRequiredColumns(scanner);
      for (var entry : scanner) {
        requiredColFQs = new HashSet<>(requiredColFQs());
        requiredColFams = new HashSet<>(requiredColFams());
        SortedMap<Key,Value> rowMap;
        try {
          rowMap = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        for (var e : rowMap.entrySet()) {
          var key = e.getKey();
          boolean removed =
              requiredColFQs.remove(new ColumnFQ(key.getColumnFamily(), key.getColumnQualifier()));
          if (!removed) {
            requiredColFams.remove(key.getColumnFamily());
          }
        }
        if (!requiredColFQs.isEmpty() || !requiredColFams.isEmpty()) {
          log.warn("Tablet {} is missing required columns: col FQs: {}, col fams: {} in the {}\n",
              entry.getKey().getRow(), requiredColFQs, requiredColFams, scanning());
          missingReqCol = true;
        }
      }
    }

    if (!missingReqCol) {
      log.trace("...The {} contains all required columns for all tablets\n", scanning());
      return true;
    } else {
      return false;
    }
  }

  /**
   * Ensures each column in the root or metadata table (or in ZK for the root metadata) is valid -
   * no unexpected columns, and for the columns that are expected, ensures the values are valid
   */
  default boolean checkColumns(ServerContext context,
      Iterator<AbstractMap.SimpleImmutableEntry<Key,Value>> iter) {
    boolean invalidCol = false;
    MetadataConstraints mc = new MetadataConstraints();

    log.trace("Scanning the {} for invalid columns...\n", scanning());
    while (iter.hasNext()) {
      var entry = iter.next();
      Key key = entry.getKey();
      // create a mutation that's equivalent to the existing data to check validity
      Mutation m = new Mutation(key.getRow());
      m.at().family(key.getColumnFamily()).qualifier(key.getColumnQualifier())
          .visibility(key.getColumnVisibility()).timestamp(key.getTimestamp())
          .put(entry.getValue());
      var violations = mc.check(new ConstraintEnv(context), m);
      if (!violations.isEmpty()) {
        violations.forEach(violationCode -> log.warn(mc.getViolationDescription(violationCode)));
        invalidCol = true;
      }
    }

    if (!invalidCol) {
      log.trace("...All columns in the {} are valid\n", scanning());
      return true;
    } else {
      return false;
    }
  }

  default void fetchRequiredColumns(Scanner scanner) {
    for (var reqColFQ : requiredColFQs()) {
      scanner.fetchColumn(reqColFQ.getColumnFamily(), reqColFQ.getColumnQualifier());
    }
    for (var reqColFam : requiredColFams()) {
      scanner.fetchColumnFamily(reqColFam);
    }
  }

  /**
   * A {@link SystemEnvironment} whose only valid operation is
   * {@link ConstraintEnv#getServerContext()}
   */
  class ConstraintEnv implements SystemEnvironment {
    ServerContext context;

    ConstraintEnv(ServerContext context) {
      this.context = context;
    }

    @Override
    public TabletId getTablet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getUser() {
      throw new UnsupportedOperationException();
    }

    @Override
    public AuthorizationContainer getAuthorizationsContainer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ServerContext getServerContext() {
      return context;
    }
  }

  private static boolean checkTable(ServerContext context, TableId tableId,
      TreeSet<KeyExtent> tablets, Consumer<String> printInfoMethod,
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

  public static boolean checkTableEntries(ServerContext context, String tableNameToCheck,
      Consumer<String> printInfoMethod, Consumer<String> printProblemMethod) throws Exception {
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
            sawProblems =
                checkTable(context, e.getKey(), e.getValue(), printInfoMethod, printProblemMethod)
                    || sawProblems;
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
      sawProblems =
          checkTable(context, e.getKey(), e.getValue(), printInfoMethod, printProblemMethod)
              || sawProblems;
    }

    if (!sawProblems) {
      printInfoMethod.accept(
          String.format("\n...No problems found in %s (%s)", tableNameToCheck, tableCheckId));
    }
    // end METADATA table sanity check
    return sawProblems;
  }

  public static void checkMetadataAndRootTableEntries(ServerContext context) throws Exception {
    Span span = TraceUtil.startSpan(MetadataCheckRunner.class, "main");
    boolean sawProblems;
    try (Scope scope = span.makeCurrent()) {

      sawProblems = checkTableEntries(context, SystemTables.ROOT.tableName(), System.out::println,
          System.out::println);
      System.out.println();
      sawProblems = checkTableEntries(context, SystemTables.METADATA.tableName(),
          System.out::println, System.out::println) || sawProblems;
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

}
