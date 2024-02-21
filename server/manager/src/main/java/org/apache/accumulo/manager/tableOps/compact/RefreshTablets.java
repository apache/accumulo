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

package org.apache.accumulo.manager.tableOps.compact;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.bulkVer2.TabletRefresher;

public class RefreshTablets extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final NamespaceId namespaceId;
  private final byte[] startRow;
  private final byte[] endRow;

  public RefreshTablets(TableId tableId, NamespaceId namespaceId, byte[] startRow, byte[] endRow) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    TabletRefresher.refresh(manager.getContext(), manager::onlineTabletServers, fateId, tableId,
        startRow, endRow, tabletMetadata -> true);

    return new CleanUp(tableId, namespaceId, startRow, endRow);
  }
}
