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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;
import org.apache.zookeeper.KeeperException;

public class SystemConfigCheckRunner implements CheckRunner {
  private static final Admin.CheckCommand.Check check = Admin.CheckCommand.Check.SYSTEM_CONFIG;

  @Override
  public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws Exception {
    Admin.CheckCommand.CheckStatus status = Admin.CheckCommand.CheckStatus.OK;
    printRunning();

    System.out.println("\n********** Checking some references **********\n");
    status = checkTableLocks(context, status);

    printCompleted(status);
    return status;
  }

  private static Admin.CheckCommand.CheckStatus checkTableLocks(ServerContext context,
      Admin.CheckCommand.CheckStatus status)
      throws InterruptedException, KeeperException, AccumuloException, AccumuloSecurityException {
    final AdminUtil<Admin> admin = new AdminUtil<>(true);
    final String zkRoot = context.getZooKeeperRoot();
    final var zTableLocksPath = ServiceLock.path(zkRoot + Constants.ZTABLE_LOCKS);
    final String fateZkPath = zkRoot + Constants.ZFATE;
    final ZooReaderWriter zk = context.getZooReaderWriter();
    final ZooStore<Admin> zs = new ZooStore<>(fateZkPath, zk);

    System.out.println("Ensuring table and namespace locks are valid...");

    var tableIds = context.tableOperations().tableIdMap().values();
    var namespaceIds = context.namespaceOperations().namespaceIdMap().values();
    List<String> lockedIds = zk.getChildren(zTableLocksPath.toString());
    boolean locksExist = !lockedIds.isEmpty();

    if (locksExist) {
      lockedIds.removeAll(tableIds);
      lockedIds.removeAll(namespaceIds);
      if (!lockedIds.isEmpty()) {
        status = Admin.CheckCommand.CheckStatus.FAILED;
        System.out
            .println("...Some table and namespace locks are INVALID (the table/namespace DNE): "
                + lockedIds);
      } else {
        System.out.println("...locks are valid");
      }
    } else {
      System.out.println("...no locks present");
    }

    System.out.println("Ensuring table and namespace locks are associated with a FATE op...");

    if (locksExist) {
      final var fateStatus = admin.getStatus(zs, zk, zTableLocksPath, null, null);
      if (!fateStatus.getDanglingHeldLocks().isEmpty()
          || !fateStatus.getDanglingWaitingLocks().isEmpty()) {
        status = Admin.CheckCommand.CheckStatus.FAILED;
        System.out.println("The following locks did not have an associated FATE operation\n");
        for (Map.Entry<String,List<String>> entry : fateStatus.getDanglingHeldLocks().entrySet()) {
          System.out.println("txid: " + entry.getKey() + " locked: " + entry.getValue());
        }
        for (Map.Entry<String,List<String>> entry : fateStatus.getDanglingWaitingLocks()
            .entrySet()) {
          System.out.println("txid: " + entry.getKey() + " locking: " + entry.getValue());
        }
      } else {
        System.out.println("...locks are valid");
      }
    } else {
      System.out.println("...no locks present");
    }

    return status;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }
}
