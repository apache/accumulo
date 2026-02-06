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

import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.RemoveEntriesForMissingFiles;
import org.apache.accumulo.server.util.adminCommand.CheckServer.Check;
import org.apache.accumulo.server.util.adminCommand.CheckServer.CheckStatus;

public class SystemFilesCheckRunner implements CheckRunner {
  private static final Check check = Check.SYSTEM_FILES;

  @Override
  public CheckStatus runCheck(ServerContext context, ServerUtilOpts opts, boolean fixFiles)
      throws Exception {
    CheckStatus status = CheckStatus.OK;
    printRunning();

    log.trace("********** Looking for missing system files **********");
    if (RemoveEntriesForMissingFiles.checkTable(context, SystemTables.METADATA.tableName(),
        fixFiles, log::trace, log::warn) != 0) {
      status = CheckStatus.FAILED;
    }

    printCompleted(status);
    return status;
  }

  @Override
  public Check getCheck() {
    return check;
  }
}
