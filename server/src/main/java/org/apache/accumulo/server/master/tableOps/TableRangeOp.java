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
package org.apache.accumulo.server.master.tableOps;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeInfo.Operation;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 * Merge makes things hard.
 * 
 * Typically, a client will read the list of tablets, and begin an operation on that tablet at the location listed in the metadata table. When a tablet splits,
 * the information read from the metadata table doesn't match reality, so the operation fails, and must be retried. But the operation will take place either on
 * the parent, or at a later time on the children. It won't take place on just half of the tablet.
 * 
 * However, when a merge occurs, the operation may have succeeded on one section of the merged area, and not on the others, when the merge occurs. There is no
 * way to retry the request at a later time on an unmodified tablet.
 * 
 * The code below uses read-write lock to prevent some operations while a merge is taking place. Normal operations, like bulk imports, will grab the read lock
 * and prevent merges (writes) while they run. Merge operations will lock out some operations while they run.
 */

class MakeDeleteEntries extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  private static final long ARBITRARY_BUFFER_SIZE = 10000;
  private static final long ARBITRARY_LATENCY = 1000;

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.info("creating delete entries for merged metadata tablets");
    Instance instance = master.getInstance();
    Connector conn = instance.getConnector(SecurityConstants.getSystemCredentials());
    BatchWriter bw = conn.createBatchWriter(Constants.METADATA_TABLE_NAME, ARBITRARY_BUFFER_SIZE, ARBITRARY_LATENCY, 1);
    AccumuloConfiguration conf = instance.getConfiguration();
    String tableDir = Constants.getMetadataTableDir(conf);
    for (FileStatus fs : master.getFileSystem().listStatus(new Path(tableDir))) {
      // TODO: add the entries only if there are no !METADATA table references
      if (fs.isDir() && fs.getPath().getName().matches("^" + Constants.GENERATED_TABLET_DIRECTORY_PREFIX + ".*")) {
        bw.addMutation(MetadataTable.createDeleteMutation(Constants.METADATA_TABLE_ID, "/" + fs.getPath().getName()));
      }
    }
    bw.close();
    return null;
  }
}

class TableRangeOpWait extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  private String tableId;
  
  public TableRangeOpWait(String tableId) {
    this.tableId = tableId;
  }
  
  @Override
  public long isReady(long tid, Master env) throws Exception {
    Text tableIdText = new Text(tableId);
    if (!env.getMergeInfo(tableIdText).getState().equals(MergeState.NONE)) {
      return 50;
    }
    return 0;
  }
  
  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    Text tableIdText = new Text(tableId);
    MergeInfo mergeInfo = env.getMergeInfo(tableIdText);
    log.warn("removing merge information " + mergeInfo);
    env.clearMergeState(tableIdText);
    Utils.unreserveTable(tableId, tid, true);
    // We can't add entries to the metadata table if it is offline for this merge.
    // If the delete entries for the metadata table were in the root tablet, it would work just fine
    // but all the delete entries go into the end of the metadata table. Work around: add the
    // delete entries after the merge completes.
    if (mergeInfo.getOperation().equals(Operation.MERGE) && tableId.equals(Constants.METADATA_TABLE_ID)) {
      return new MakeDeleteEntries();
    }
    return null;
  }
  
}

public class TableRangeOp extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  
  private String tableId;
  private byte[] startRow;
  private byte[] endRow;
  private Operation op;
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveTable(tableId, tid, true, true, TableOperation.MERGE);
  }
  
  public TableRangeOp(MergeInfo.Operation op, String tableId, Text startRow, Text endRow) throws ThriftTableOperationException {
    
    this.tableId = tableId;
    this.startRow = TextUtil.getBytes(startRow);
    this.endRow = TextUtil.getBytes(endRow);
    this.op = op;
  }
  
  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    
    Text start = startRow.length == 0 ? null : new Text(startRow);
    Text end = endRow.length == 0 ? null : new Text(endRow);
    Text tableIdText = new Text(tableId);
    
    if (start != null && end != null)
      if (start.compareTo(end) >= 0)
        throw new ThriftTableOperationException(tableId, null, TableOperation.MERGE, TableOperationExceptionType.BAD_RANGE,
            "start row must be less than end row");
    
    env.mustBeOnline(tableId);
    
    MergeInfo info = env.getMergeInfo(tableIdText);
    
    if (info.getState() == MergeState.NONE) {
      KeyExtent range = new KeyExtent(tableIdText, end, start);
      env.setMergeState(new MergeInfo(range, op), MergeState.STARTED);
    }
    
    return new TableRangeOpWait(tableId);
  }
  
  @Override
  public void undo(long tid, Master env) throws Exception {
    // Not sure this is a good thing to do. The Master state engine should be the one to remove it.
    Text tableIdText = new Text(tableId);
    MergeInfo mergeInfo = env.getMergeInfo(tableIdText);
    if (mergeInfo.getState() != MergeState.NONE)
      log.warn("removing merge information " + mergeInfo);
    env.clearMergeState(tableIdText);
    Utils.unreserveTable(tableId, tid, true);
  }
  
}
