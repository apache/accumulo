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

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Repo asks hosted tablets that were bulk loaded into to refresh their metadata. It works by
 * getting a metadata snapshot once that includes tablets and their locations. Then it repeatedly
 * ask the tablets at those locations to refresh their metadata. It the tablets are no longer the
 * location its ok. That means the tablet either unloaded before of after the snapshot. In either
 * case the tablet will see the bulk files the next time its hosted somewhere.
 */
public class RefreshTablets extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(RefreshTablets.class);

  private static final long serialVersionUID = 1L;

  private final BulkInfo bulkInfo;

  public RefreshTablets(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {
    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {

    // ELASTICITY_TODO DEFERRED - ISSUE 4044
    TabletRefresher.refresh(manager.getContext(), manager::onlineTabletServers, fateId.getTid(),
        bulkInfo.tableId, bulkInfo.firstSplit, bulkInfo.lastSplit,
        tabletMetadata -> tabletMetadata.getLoaded().containsValue(fateId.getTid()));

    return new CleanUpBulkImport(bulkInfo);
  }
}
