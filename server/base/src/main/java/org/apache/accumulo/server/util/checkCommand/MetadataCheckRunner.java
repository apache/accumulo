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
import org.apache.accumulo.core.client.TableNotFoundException;
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
import org.apache.accumulo.server.util.Admin;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

public interface MetadataCheckRunner extends CheckRunner {

  String tableName();

  TableId tableId();

  Set<ColumnFQ> requiredColFQs();

  Set<Text> requiredColFams();

  default String scanning() {
    return String.format("%s (%s) table", tableName(), tableId());
  }

  /**
   * Ensures that the {@link #tableName()} table (either metadata or root table) has all columns
   * that are expected. For the root metadata, ensures that the expected "columns" exist in ZK.
   */
  default Admin.CheckCommand.CheckStatus checkRequiredColumns(ServerContext context,
      Admin.CheckCommand.CheckStatus status)
      throws TableNotFoundException, InterruptedException, KeeperException {
    Set<ColumnFQ> requiredColFQs;
    Set<Text> requiredColFams;
    boolean missingReqCol = false;

    System.out.printf("Scanning the %s for missing required columns...\n", scanning());
    try (Scanner scanner = context.createScanner(tableName(), Authorizations.EMPTY)) {
      var is = new IteratorSetting(100, "tablets", WholeRowIterator.class);
      scanner.addScanIterator(is);
      scanner.setRange(MetadataSchema.TabletsSection.getRange());
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
          System.out.printf(
              "Tablet %s is missing required columns: col FQs: %s, col fams: %s in the %s\n",
              entry.getKey().getRow(), requiredColFQs, requiredColFams, scanning());
          status = Admin.CheckCommand.CheckStatus.FAILED;
          missingReqCol = true;
        }
      }
    }

    if (!missingReqCol) {
      System.out.printf("...The %s contains all required columns for all tablets\n", scanning());
    }
    return status;
  }

  /**
   * Ensures each column in the root or metadata table (or in ZK for the root metadata) is valid -
   * no unexpected columns, and for the columns that are expected, ensures the values are valid
   */
  default Admin.CheckCommand.CheckStatus checkColumns(ServerContext context,
      Iterator<AbstractMap.SimpleImmutableEntry<Key,Value>> iter,
      Admin.CheckCommand.CheckStatus status)
      throws TableNotFoundException, InterruptedException, KeeperException {
    boolean invalidCol = false;

    System.out.printf("Scanning the %s for invalid columns...\n", scanning());
    while (iter.hasNext()) {
      var entry = iter.next();
      Key key = entry.getKey();
      // create a mutation that's equivalent to the existing data to check validity
      Mutation m = new Mutation(key.getRow());
      m.at().family(key.getColumnFamily()).qualifier(key.getColumnQualifier())
          .visibility(key.getColumnVisibility()).timestamp(key.getTimestamp())
          .put(entry.getValue());
      MetadataConstraints mc = new MetadataConstraints();
      var violations = mc.check(new ConstraintEnv(context), m);
      if (!violations.isEmpty()) {
        violations.forEach(
            violationCode -> System.out.println(mc.getViolationDescription(violationCode)));
        status = Admin.CheckCommand.CheckStatus.FAILED;
        invalidCol = true;
      }
    }

    if (!invalidCol) {
      System.out.printf("...All columns in the %s are valid\n", scanning());
    }
    return status;
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
