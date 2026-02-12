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
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.CheckForMetadataProblems;
import org.apache.accumulo.server.util.FindOfflineTablets;
import org.apache.accumulo.server.util.adminCommand.SystemCheck.Check;
import org.apache.accumulo.server.util.adminCommand.SystemCheck.CheckStatus;
import org.apache.hadoop.io.Text;

public class MetadataTableCheckRunner implements MetadataCheckRunner {
  private static final Check check = Check.METADATA_TABLE;

  @Override
  public String tableName() {
    return SystemTables.METADATA.tableName();
  }

  @Override
  public TableId tableId() {
    return SystemTables.METADATA.tableId();
  }

  @Override
  public Set<Text> requiredColFams() {
    return Set.of();
  }

  @Override
  public CheckStatus runCheck(ServerContext context, ServerUtilOpts opts, boolean fixFiles)
      throws Exception {
    CheckStatus status = CheckStatus.OK;
    printRunning();

    log.trace("********** Looking for offline tablets **********");
    if (FindOfflineTablets.findOffline(context, null, true, true, log::trace, log::warn) != 0) {
      status = CheckStatus.FAILED;
    } else {
      log.trace("All good... No offline tablets found");
    }

    log.trace("********** Checking some references **********");
    if (CheckForMetadataProblems.checkMetadataAndRootTableEntries(tableName(), opts, log::trace,
        log::warn)) {
      status = CheckStatus.FAILED;
    }

    log.trace("********** Looking for missing columns **********");
    status = checkRequiredColumns(context, status);

    log.trace("********** Looking for invalid columns **********");
    try (Scanner scanner = context.createScanner(tableName(), Authorizations.EMPTY)) {
      status = checkColumns(context,
          scanner.stream().map(AbstractMap.SimpleImmutableEntry::new).iterator(), status);
    }

    printCompleted(status);
    return status;
  }

  @Override
  public Check getCheck() {
    return check;
  }
}
