/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tableOps.bulkVer2;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;

public class CompleteBulkImport extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private BulkInfo info;

  public CompleteBulkImport(BulkInfo info) {
    this.info = info;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    ZooArbitrator.stop(master.getContext(), Constants.BULK_ARBITRATOR_TYPE, tid);
    return new CleanUpBulkImport(info);
  }
}
