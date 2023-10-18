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
package org.apache.accumulo.manager.tableOps.split;

import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.DestructiveTabletManagerRepo;

public class DeleteOperationIds extends DestructiveTabletManagerRepo {
  private static final long serialVersionUID = 1L;
  private final SplitInfo splitInfo;

  public DeleteOperationIds(SplitInfo splitInfo) {
    super(TabletOperationType.SPLITTING, splitInfo.getOriginal());
    this.splitInfo = splitInfo;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    removeOperationIds(tid, manager, splitInfo.getTablets());

    // Get the tablets hosted ASAP if necessary.
    manager.getEventCoordinator().event(splitInfo.getOriginal(), "Added %d splits to %s",
        splitInfo.getSplits().size(), splitInfo.getOriginal());

    TabletLogger.split(splitInfo.getOriginal(), splitInfo.getSplits());

    return null;
  }

  @Override
  public String getReturn() {
    return TableOperationsImpl.SPLIT_SUCCESS_MSG;
  }
}
