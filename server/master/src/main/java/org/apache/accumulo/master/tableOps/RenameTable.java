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
package org.apache.accumulo.master.tableOps;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.slf4j.LoggerFactory;

public class RenameTable extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private Table.ID tableId;
  private Namespace.ID namespaceId;
  private String oldTableName;
  private String newTableName;

  @Override
  public long isReady(long tid, Master env) throws Exception {
    return Utils.reserveNamespace(namespaceId, tid, false, true, TableOperation.RENAME) + Utils.reserveTable(tableId, tid, true, true, TableOperation.RENAME);
  }

  public RenameTable(Namespace.ID namespaceId, Table.ID tableId, String oldTableName, String newTableName) throws NamespaceNotFoundException {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
    this.oldTableName = oldTableName;
    this.newTableName = newTableName;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    Instance instance = master.getInstance();
    Pair<String,String> qualifiedOldTableName = Tables.qualify(oldTableName);
    Pair<String,String> qualifiedNewTableName = Tables.qualify(newTableName);

    // ensure no attempt is made to rename across namespaces
    if (newTableName.contains(".") && !namespaceId.equals(Namespaces.getNamespaceId(instance, qualifiedNewTableName.getFirst())))
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), oldTableName, TableOperation.RENAME, TableOperationExceptionType.INVALID_NAME,
          "Namespace in new table name does not match the old table name");

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    Utils.tableNameLock.lock();
    try {
      Utils.checkTableDoesNotExist(instance, newTableName, tableId, TableOperation.RENAME);

      final String newName = qualifiedNewTableName.getSecond();
      final String oldName = qualifiedOldTableName.getSecond();

      final String tap = ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME;

      zoo.mutate(tap, null, null, new Mutator() {
        @Override
        public byte[] mutate(byte[] current) throws Exception {
          final String currentName = new String(current, UTF_8);
          if (currentName.equals(newName))
            return null; // assume in this case the operation is running again, so we are done
          if (!currentName.equals(oldName)) {
            throw new AcceptableThriftTableOperationException(null, oldTableName, TableOperation.RENAME, TableOperationExceptionType.NOTFOUND,
                "Name changed while processing");
          }
          return newName.getBytes(UTF_8);
        }
      });
      Tables.clearCache(instance);
    } finally {
      Utils.tableNameLock.unlock();
      Utils.unreserveTable(tableId, tid, true);
      Utils.unreserveNamespace(namespaceId, tid, false);
    }

    LoggerFactory.getLogger(RenameTable.class).debug("Renamed table {} {} {}", tableId, oldTableName, newTableName);

    return null;
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveTable(tableId, tid, true);
    Utils.unreserveNamespace(namespaceId, tid, false);
  }

}
