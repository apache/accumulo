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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.slf4j.LoggerFactory;

public class RenameNamespace extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private Namespace.ID namespaceId;
  private String oldName;
  private String newName;

  @Override
  public long isReady(long id, Master environment) throws Exception {
    return Utils.reserveNamespace(namespaceId, id, true, true, TableOperation.RENAME);
  }

  public RenameNamespace(Namespace.ID namespaceId, String oldName, String newName) {
    this.namespaceId = namespaceId;
    this.oldName = oldName;
    this.newName = newName;
  }

  @Override
  public Repo<Master> call(long id, Master master) throws Exception {

    Instance instance = master.getInstance();

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    Utils.tableNameLock.lock();
    try {
      Utils.checkNamespaceDoesNotExist(instance, newName, namespaceId, TableOperation.RENAME);

      final String tap = ZooUtil.getRoot(instance) + Constants.ZNAMESPACES + "/" + namespaceId + Constants.ZNAMESPACE_NAME;

      zoo.mutate(tap, null, null, new Mutator() {
        @Override
        public byte[] mutate(byte[] current) throws Exception {
          final String currentName = new String(current);
          if (currentName.equals(newName))
            return null; // assume in this case the operation is running again, so we are done
          if (!currentName.equals(oldName)) {
            throw new AcceptableThriftTableOperationException(null, oldName, TableOperation.RENAME, TableOperationExceptionType.NAMESPACE_NOTFOUND,
                "Name changed while processing");
          }
          return newName.getBytes();
        }
      });
      Tables.clearCache(instance);
    } finally {
      Utils.tableNameLock.unlock();
      Utils.unreserveNamespace(namespaceId, id, true);
    }

    LoggerFactory.getLogger(RenameNamespace.class).debug("Renamed namespace {} {} {}", namespaceId, oldName, newName);

    return null;
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveNamespace(namespaceId, tid, true);
  }

}
