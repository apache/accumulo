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

import java.util.AbstractMap;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.CheckForMetadataProblems;
import org.apache.accumulo.server.util.FindOfflineTablets;
import org.apache.hadoop.io.Text;

public class RootTableCheckRunner implements MetadataCheckRunner {
  private static final Admin.CheckCommand.Check check = Admin.CheckCommand.Check.ROOT_TABLE;

  @Override
  public String tableName() {
    return AccumuloTable.ROOT.tableName();
  }

  @Override
  public TableId tableId() {
    return AccumuloTable.ROOT.tableId();
  }

  @Override
  public Set<ColumnFQ> requiredColFQs() {
    return Set.of(MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.LOCK_COLUMN);
  }

  @Override
  public Set<Text> requiredColFams() {
    return Set.of(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
  }

  @Override
  public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws Exception {
    Admin.CheckCommand.CheckStatus status = Admin.CheckCommand.CheckStatus.OK;
    printRunning();

    System.out.println("\n********** Looking for offline tablets **********\n");
    if (FindOfflineTablets.findOffline(context, AccumuloTable.METADATA.tableName(), true, false)
        != 0) {
      status = Admin.CheckCommand.CheckStatus.FAILED;
    } else {
      System.out.println("All good... No offline tablets found");
    }

    System.out.println("\n********** Checking some references **********\n");
    if (CheckForMetadataProblems.checkMetadataAndRootTableEntries(tableName(), opts)) {
      status = Admin.CheckCommand.CheckStatus.FAILED;
    }

    System.out.println("\n********** Looking for missing columns **********\n");
    status = checkRequiredColumns(context, status);

    System.out.println("\n********** Looking for invalid columns **********\n");
    try (Scanner scanner = context.createScanner(tableName(), Authorizations.EMPTY)) {
      status = checkColumns(context,
          scanner.stream().map(AbstractMap.SimpleImmutableEntry::new).iterator(), status);
    }

    printCompleted(status);
    return status;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }
}
