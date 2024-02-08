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
package org.apache.accumulo.manager.compaction.coordinator.commit;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.bulkVer2.TabletRefresher;

import com.google.common.util.concurrent.MoreExecutors;

public class RefreshTablet extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private final TKeyExtent extent;
  private final String tserverInstance;

  public RefreshTablet(TKeyExtent extent, String tserverInstance) {
    this.extent = extent;
    this.tserverInstance = tserverInstance;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {

    TServerInstance tsi = new TServerInstance(tserverInstance);

    // there is a single tserver and single tablet, do not need a thread pool. The direct executor
    // will run everything in the current thread
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    try {
      TabletRefresher.refreshTablets(executorService, "compaction:" + KeyExtent.fromThrift(extent),
          manager.getContext(), manager::onlineTabletServers,
          Map.of(TabletMetadata.Location.current(tsi), List.of(extent)));
    } finally {
      executorService.shutdownNow();
    }

    return null;
  }
}
