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

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;
import org.apache.zookeeper.KeeperException;

public class TableLocksCheckRunner implements CheckRunner {
  private static final Admin.CheckCommand.Check check = Admin.CheckCommand.Check.TABLE_LOCKS;

  @Override
  public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws Exception {
    Admin.CheckCommand.CheckStatus status = Admin.CheckCommand.CheckStatus.OK;
    printRunning();

    log.trace("********** Checking some references **********");
    status = checkTableLocks(context, status);

    printCompleted(status);
    return status;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }

  private static Admin.CheckCommand.CheckStatus checkTableLocks(ServerContext context,
      Admin.CheckCommand.CheckStatus status)
      throws InterruptedException, KeeperException, AccumuloException, AccumuloSecurityException {
    final AdminUtil<Admin> admin = new AdminUtil<>();
    final var zTableLocksPath = context.getServerPaths().createTableLocksPath();
    final var zk = context.getZooSession();
    final MetaFateStore<Admin> mfs = new MetaFateStore<>(zk, null, null);
    final UserFateStore<Admin> ufs =
        new UserFateStore<>(context, AccumuloTable.FATE.tableName(), null, null);

    log.trace("Ensuring table and namespace locks are valid...");

    var tableIds = context.tableOperations().tableIdMap().values();
    var namespaceIds = context.namespaceOperations().namespaceIdMap().values();
    List<String> lockedIds =
        context.getZooSession().asReader().getChildren(zTableLocksPath.toString());
    boolean locksExist = !lockedIds.isEmpty();

    if (locksExist) {
      lockedIds.removeAll(tableIds);
      lockedIds.removeAll(namespaceIds);
      if (!lockedIds.isEmpty()) {
        status = Admin.CheckCommand.CheckStatus.FAILED;
        log.warn("...Some table and namespace locks are INVALID (the table/namespace DNE): "
            + lockedIds);
      } else {
        log.trace("...locks are valid");
      }
    } else {
      log.trace("...no locks present");
    }

    log.trace("Ensuring table and namespace locks are associated with a FATE op...");

    if (locksExist) {
      final var fateStatus =
          admin.getStatus(Map.of(FateInstanceType.META, mfs, FateInstanceType.USER, ufs), zk,
              zTableLocksPath, null, null, null);
      if (!fateStatus.getDanglingHeldLocks().isEmpty()
          || !fateStatus.getDanglingWaitingLocks().isEmpty()) {
        status = Admin.CheckCommand.CheckStatus.FAILED;
        log.warn("The following locks did not have an associated FATE operation\n");
        for (Map.Entry<FateId,List<String>> entry : fateStatus.getDanglingHeldLocks().entrySet()) {
          log.warn("fateId: " + entry.getKey() + " locked: " + entry.getValue());
        }
        for (Map.Entry<FateId,List<String>> entry : fateStatus.getDanglingWaitingLocks()
            .entrySet()) {
          log.warn("fateId: " + entry.getKey() + " locking: " + entry.getValue());
        }
      } else {
        log.trace("...locks are valid");
      }
    } else {
      log.trace("...no locks present");
    }

    return status;
  }
}
