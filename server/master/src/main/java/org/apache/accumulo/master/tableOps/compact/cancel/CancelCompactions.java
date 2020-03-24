/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tableOps.compact.cancel;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter.Mutator;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;

public class CancelCompactions extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private TableId tableId;
  private NamespaceId namespaceId;

  public CancelCompactions(NamespaceId namespaceId, TableId tableId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, tid, false, true, TableOperation.COMPACT_CANCEL)
        + Utils.reserveTable(env, tableId, tid, false, true, TableOperation.COMPACT_CANCEL);
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    String zCompactID = Constants.ZROOT + "/" + environment.getInstanceID() + Constants.ZTABLES
        + "/" + tableId + Constants.ZTABLE_COMPACT_ID;
    String zCancelID = Constants.ZROOT + "/" + environment.getInstanceID() + Constants.ZTABLES + "/"
        + tableId + Constants.ZTABLE_COMPACT_CANCEL_ID;

    ZooReaderWriter zoo = environment.getContext().getZooReaderWriter();

    byte[] currentValue = zoo.getData(zCompactID, null);

    String cvs = new String(currentValue, UTF_8);
    String[] tokens = cvs.split(",");
    final long flushID = Long.parseLong(tokens[0]);

    zoo.mutate(zCancelID, null, null, new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) {
        long cid = Long.parseLong(new String(currentValue, UTF_8));

        if (cid < flushID)
          return Long.toString(flushID).getBytes(UTF_8);
        else
          return Long.toString(cid).getBytes(UTF_8);
      }
    });

    return new FinishCancelCompaction(namespaceId, tableId);
  }

  @Override
  public void undo(long tid, Master env) {
    Utils.unreserveTable(env, tableId, tid, false);
    Utils.unreserveNamespace(env, namespaceId, tid, false);
  }
}
