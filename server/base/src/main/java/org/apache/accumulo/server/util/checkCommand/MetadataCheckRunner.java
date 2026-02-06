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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.constraints.SystemEnvironment;
import org.apache.accumulo.server.util.adminCommand.CheckServer.CheckStatus;
import org.apache.hadoop.io.Text;

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
  default CheckStatus checkRequiredColumns(ServerContext context, CheckStatus status)
      throws Exception {
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
          status = CheckStatus.FAILED;
          missingReqCol = true;
        }
      }
    }

    if (!missingReqCol) {
      log.trace("...The {} contains all required columns for all tablets\n", scanning());
    }
    return status;
  }

  /**
   * Ensures each column in the root or metadata table (or in ZK for the root metadata) is valid -
   * no unexpected columns, and for the columns that are expected, ensures the values are valid
   */
  default CheckStatus checkColumns(ServerContext context,
      Iterator<AbstractMap.SimpleImmutableEntry<Key,Value>> iter, CheckStatus status) {
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
        status = CheckStatus.FAILED;
        invalidCol = true;
      }
    }

    if (!invalidCol) {
      log.trace("...All columns in the {} are valid\n", scanning());
    }
    return status;
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
}
