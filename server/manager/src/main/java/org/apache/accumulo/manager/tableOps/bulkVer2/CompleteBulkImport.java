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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;

public class CompleteBulkImport extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private BulkInfo info;

  public CompleteBulkImport(BulkInfo info) {
    this.info = info;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    ZooArbitrator.stop(manager.getContext(), Constants.BULK_ARBITRATOR_TYPE, tid);
    return new CleanUpBulkImport(info);
  }
}
