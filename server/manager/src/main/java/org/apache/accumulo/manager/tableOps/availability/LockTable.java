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
package org.apache.accumulo.manager.tableOps.availability;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockTable extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LockTable.class);

  private final TableId tableId;
  private final NamespaceId namespaceId;
  private final TRange tRange;
  private final TabletAvailability tabletAvailability;

  public LockTable(TableId tableId, NamespaceId namespaceId, TRange range,
      TabletAvailability tabletAvailability) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.tRange = range;
    this.tabletAvailability = tabletAvailability;
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {
    return Utils.reserveNamespace(manager, namespaceId, fateId,
        DistributedReadWriteLock.LockType.READ, true, TableOperation.SET_TABLET_AVAILABILITY)
        + Utils.reserveTable(manager, tableId, fateId, DistributedReadWriteLock.LockType.WRITE,
            true, TableOperation.SET_TABLET_AVAILABILITY);
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    return new SetTabletAvailability(tableId, namespaceId, tRange, tabletAvailability);
  }

  @Override
  public void undo(FateId fateId, Manager manager) throws Exception {
    Utils.unreserveNamespace(manager, namespaceId, fateId, DistributedReadWriteLock.LockType.READ);
    Utils.unreserveTable(manager, tableId, fateId, DistributedReadWriteLock.LockType.WRITE);
  }
}
