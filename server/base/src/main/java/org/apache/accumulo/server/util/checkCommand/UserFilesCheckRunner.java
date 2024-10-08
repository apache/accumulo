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

import static org.apache.accumulo.server.util.Admin.CheckCommand.Check;

import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.RemoveEntriesForMissingFiles;

public class UserFilesCheckRunner implements CheckRunner {
  private static final Check check = Check.USER_FILES;

  @Override
  public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws Exception {
    Admin.CheckCommand.CheckStatus status = Admin.CheckCommand.CheckStatus.OK;
    printRunning();

    System.out.println("\n********** Looking for missing user files **********\n");
    for (String tableName : context.tableOperations().list()) {
      var tableId = context.getTableId(tableName);
      if (!AccumuloTable.allTableIds().contains(context.getTableId(tableName))) {
        System.out.printf("Checking table %s (%s) for missing files\n", tableName, tableId);
        if (RemoveEntriesForMissingFiles.checkTable(context, tableName, fixFiles) != 0) {
          status = Admin.CheckCommand.CheckStatus.FAILED;
        }
      }
    }

    printCompleted(status);
    return status;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }
}
