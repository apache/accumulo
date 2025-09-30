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
package org.apache.accumulo.manager.tableOps.create;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.manager.tableOps.AbstractRepo;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.TableInfo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.util.PropUtil;

class PopulateZookeeper extends AbstractRepo {

  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;

  PopulateZookeeper(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(FateId fateId, FateEnv environment) throws Exception {
    return Utils.reserveTable(environment.getContext(), tableInfo.getTableId(), fateId,
        LockType.WRITE, false, TableOperation.CREATE);
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {
    // reserve the table name in zookeeper or fail

    var context = env.getContext();
    // write tableName & tableId, first to Table Mapping and then to Zookeeper
    context.getTableMapping(tableInfo.getNamespaceId()).put(tableInfo.getTableId(),
        tableInfo.getTableName(), TableOperation.CREATE);
    env.getTableManager().addTable(tableInfo.getTableId(), tableInfo.getNamespaceId(),
        tableInfo.getTableName());

    try {
      PropUtil.setProperties(context, TablePropKey.of(tableInfo.getTableId()), tableInfo.props);
    } catch (IllegalStateException ex) {
      throw new ThriftTableOperationException(null, tableInfo.getTableName(), TableOperation.CREATE,
          TableOperationExceptionType.OTHER, "Property or value not valid for create "
              + tableInfo.getTableName() + " in " + tableInfo.props);
    }

    context.clearTableListCache();
    return new ChooseDir(tableInfo);

  }

  @Override
  public void undo(FateId fateId, FateEnv env) throws Exception {
    env.getTableManager().removeTable(tableInfo.getTableId(), tableInfo.getNamespaceId());
    Utils.unreserveTable(env.getContext(), tableInfo.getTableId(), fateId, LockType.WRITE);
    env.getContext().clearTableListCache();
  }

}
