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

import static com.google.common.base.Charsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

class FinishCancelCompaction extends MasterRepo {
  private static final long serialVersionUID = 1L;
  private String tableId;

  public FinishCancelCompaction(String tableId) {
    this.tableId = tableId;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    Utils.getReadLock(tableId, tid).unlock();
    return null;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {

  }
}

/**
 *
 */
public class CancelCompactions extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private String tableId;
  private String namespaceId;

  public CancelCompactions(String tableId) {
    this.tableId = tableId;
    Instance inst = HdfsZooInstance.getInstance();
    this.namespaceId = Tables.getNamespaceId(inst, tableId);
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveNamespace(namespaceId, tid, false, true, TableOperation.COMPACT_CANCEL)
        + Utils.reserveTable(tableId, tid, false, true, TableOperation.COMPACT_CANCEL);
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    String zCompactID = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_COMPACT_ID;
    String zCancelID = Constants.ZROOT + "/" + HdfsZooInstance.getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_COMPACT_CANCEL_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();

    byte[] currentValue = zoo.getData(zCompactID, null);

    String cvs = new String(currentValue, UTF_8);
    String[] tokens = cvs.split(",");
    final long flushID = Long.parseLong(tokens[0]);

    zoo.mutate(zCancelID, null, null, new Mutator() {
      @Override
      public byte[] mutate(byte[] currentValue) throws Exception {
        long cid = Long.parseLong(new String(currentValue, UTF_8));

        if (cid < flushID)
          return Long.toString(flushID).getBytes(UTF_8);
        else
          return Long.toString(cid).getBytes(UTF_8);
      }
    });

    return new FinishCancelCompaction(tableId);
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    Utils.unreserveNamespace(namespaceId, tid, false);
    Utils.unreserveTable(tableId, tid, false);
  }
}
