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
package org.apache.accumulo.master.tableOps.create;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.TableInfo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

class PopulateMetadata extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;

  PopulateMetadata(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    KeyExtent extent = new KeyExtent(tableInfo.getTableId(), null, null);
    MetadataTableUtil.addTablet(extent, ServerColumnFamily.DEFAULT_TABLET_DIR_NAME,
        environment.getContext(), tableInfo.getTimeType(), environment.getMasterLock());

    if (tableInfo.getInitialSplitSize() > 0) {
      SortedSet<Text> splits =
          Utils.getSortedSetFromFile(environment.getInputStream(tableInfo.getSplitFile()), true);
      SortedSet<Text> dirs = Utils
          .getSortedSetFromFile(environment.getInputStream(tableInfo.getSplitDirsFile()), false);
      Map<Text,Text> splitDirMap = createSplitDirectoryMap(splits, dirs);
      try (BatchWriter bw = environment.getContext().createBatchWriter("accumulo.metadata")) {
        writeSplitsToMetadataTable(environment.getContext(), tableInfo.getTableId(), splits,
            splitDirMap, tableInfo.getTimeType(), environment.getMasterLock(), bw);
      }
    }
    return new FinishCreateTable(tableInfo);
  }

  private void writeSplitsToMetadataTable(ServerContext ctx, TableId tableId,
      SortedSet<Text> splits, Map<Text,Text> data, TimeType timeType, ZooLock lock, BatchWriter bw)
      throws MutationsRejectedException {
    Text prevSplit = null;
    Value dirValue;
    for (Text split : Iterables.concat(splits, Collections.singleton(null))) {
      Mutation mut = new KeyExtent(tableId, split, prevSplit).getPrevRowUpdateMutation();
      dirValue = (split == null) ? new Value(ServerColumnFamily.DEFAULT_TABLET_DIR_NAME)
          : new Value(data.get(split));
      MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut, dirValue);
      MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut,
          new Value(new MetadataTime(0, timeType).encode()));
      MetadataTableUtil.putLockID(ctx, lock, mut);
      prevSplit = split;
      bw.addMutation(mut);
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.getTableId(), false, environment.getContext(),
        environment.getMasterLock());
  }

  /**
   * Create a map containing an association between each split directory and a split value.
   */
  private Map<Text,Text> createSplitDirectoryMap(SortedSet<Text> splits, SortedSet<Text> dirs) {
    Preconditions.checkArgument(splits.size() == dirs.size());
    Map<Text,Text> data = new HashMap<>();
    Iterator<Text> s = splits.iterator();
    Iterator<Text> d = dirs.iterator();
    while (s.hasNext() && d.hasNext()) {
      data.put(s.next(), d.next());
    }
    return data;
  }
}
