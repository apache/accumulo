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

import java.util.regex.Matcher;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.Namespaces;
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

public class UndeleteTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private TableId tableId;
  private NamespaceId namespaceId;
  private String newTableName;

  @Override
  public long isReady(long tid, Master env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, tid, false, true, TableOperation.UNDELETE)
        + Utils.reserveTable(env, tableId, tid, true, true, TableOperation.UNDELETE);
  }

  public UndeleteTable(NamespaceId namespaceId, TableId tableId, String newTableName) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
    this.newTableName = newTableName;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    final String trashTableFullName = Tables.getTrashTableName(master.getContext(), tableId);

    if (Tables.getTableState(master.getContext(), tableId) != TableState.TRASH)
      throw new AcceptableThriftTableOperationException(tableId.canonical(), trashTableFullName,
          TableOperation.UNDELETE, TableOperationExceptionType.OTHER,
          "Only tables from the Trash can be undeleted");

    Pair<String,String> qualifiedName = Tables.qualify(trashTableFullName);
    if (newTableName != null) {
      Pair<String,String> qualifiedNewTableName = Tables.qualify(newTableName);

      // ensure no attempt is made to rename across namespaces
      if (newTableName.contains(".") && !namespaceId
          .equals(Namespaces.getNamespaceId(master.getContext(), qualifiedNewTableName.getFirst())))
        throw new AcceptableThriftTableOperationException(tableId.canonical(), trashTableFullName,
            TableOperation.UNDELETE, TableOperationExceptionType.INVALID_NAME,
            "Namespace in new table name does not match the old table name");
    } else {
      Matcher matcher = Constants.TRASH_TABLE_NAME_PATTERN.matcher(qualifiedName.getSecond());
      matcher.find();
      String originalName = matcher.group(1);
      String namespace = qualifiedName.getFirst() != null && qualifiedName.getFirst().length() > 0
          ? qualifiedName.getFirst() + "." : "";
      newTableName = namespace + originalName;
    }

    final String finalName = newTableName;
    IZooReaderWriter zoo = master.getContext().getZooReaderWriter();

    Utils.getTableNameLock().lock();
    try {
      Utils.checkTableDoesNotExist(master.getContext(), newTableName, tableId,
          TableOperation.UNDELETE);

      final String tap =
          master.getZooKeeperRoot() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME;

      zoo.mutate(tap, null, null, current -> {
        final String currentName = new String(current, UTF_8);
        if (currentName.equals(finalName))
          return null; // assume in this case the operation is running again, so we are done
        if (!currentName.equals(trashTableFullName)) {
          throw new AcceptableThriftTableOperationException(null, trashTableFullName,
              TableOperation.UNDELETE, TableOperationExceptionType.NOTFOUND,
              "Name changed while processing");
        }
        return finalName.getBytes(UTF_8);
      });
      Tables.clearCache(master.getContext());
    } finally {
      Utils.getTableNameLock().unlock();
      Utils.unreserveTable(master, tableId, tid, true);
      Utils.unreserveNamespace(master, namespaceId, tid, false);
    }

    master.getTableManager().transitionTableState(tableId, TableState.ONLINE);
    master.getEventCoordinator().event("undeleting table %s ", tableId);

    LoggerFactory.getLogger(UndeleteTable.class).debug("Undelete table {} {} {}", tableId,
        newTableName, trashTableFullName);

    return null;
  }

  @Override
  public void undo(long tid, Master env) {
    Utils.unreserveTable(env, tableId, tid, true);
    Utils.unreserveNamespace(env, namespaceId, tid, false);
  }

}
