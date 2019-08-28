/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.tableOps.delete;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.slf4j.LoggerFactory;

public class TrashTable extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private static final int MAX_TABLES_WITH_MATCHING_NAMES = 100;

  private TableId tableId;
  private NamespaceId namespaceId;

  @Override
  public long isReady(long tid, Master env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, tid, false, true, TableOperation.TRASH)
        + Utils.reserveTable(env, tableId, tid, true, true, TableOperation.TRASH);
  }

  public TrashTable(NamespaceId namespaceId, TableId tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    final String tableOriginalName = Tables.getTableName(master.getContext(), tableId);
    Pair<String,String> qualifiedName = Tables.qualify(tableOriginalName);
    Date now = new Date();
    String formatedDate = new SimpleDateFormat(Constants.TRASH_DATE_FORMAT).format(now);
    String namespace = qualifiedName.getFirst() != null && qualifiedName.getFirst().length() > 0
        ? qualifiedName.getFirst() + "." : "";
    String trashTableBaseName =
        namespace + "trash_" + qualifiedName.getSecond() + "_" + formatedDate + "_";

    IZooReaderWriter zoo = master.getContext().getZooReaderWriter();

    Utils.getTableNameLock().lock();
    int counter = 1;
    try {
      boolean foundNotUsedCounter = false;
      while (!foundNotUsedCounter) {
        try {
          Utils.checkTableDoesNotExist(master.getContext(), trashTableBaseName + counter, null,
              TableOperation.TRASH);
        } catch (Exception e) {
          if (counter < MAX_TABLES_WITH_MATCHING_NAMES) {
            continue;
          }
          throw e;
        }
        foundNotUsedCounter = true;
      }

      final String trashTableName = trashTableBaseName + counter;
      final String tap =
          master.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME;

      zoo.mutate(tap, null, null, current -> {
        final String currentName = new String(current, UTF_8);
        if (currentName.equals(trashTableName))
          return null; // assume in this case the operation is running again, so we are done
        if (!currentName.equals(tableOriginalName)) {
          throw new AcceptableThriftTableOperationException(null, tableOriginalName,
              TableOperation.TRASH, TableOperationExceptionType.NOTFOUND,
              "Name changed while processing");
        }
        return trashTableName.getBytes(UTF_8);
      });
      Tables.clearCache(master.getContext());
    } finally {
      Utils.getTableNameLock().unlock();
      Utils.unreserveTable(master, tableId, tid, true);
      Utils.unreserveNamespace(master, namespaceId, tid, false);
    }

    master.getTableManager().transitionTableState(tableId, TableState.TRASH);
    master.getEventCoordinator().event("deleting table %s ", tableId);

    LoggerFactory.getLogger(TrashTable.class).debug("Trash table {} {} {}", tableId,
        trashTableBaseName + counter, tableOriginalName);

    return null;
  }

  @Override
  public void undo(long tid, Master env) {
    Utils.unreserveTable(env, tableId, tid, true);
    Utils.unreserveNamespace(env, namespaceId, tid, false);
  }

}
