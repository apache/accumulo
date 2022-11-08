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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.TableInfo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

class PopulateMetadata extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;

  PopulateMetadata(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Manager environment) {
    return 0;
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    KeyExtent extent = new KeyExtent(tableInfo.getTableId(), null, null);
    MetadataTableUtil.addTablet(extent, ServerColumnFamily.DEFAULT_TABLET_DIR_NAME,
        env.getContext(), tableInfo.getTimeType(), env.getManagerLock());

    if (tableInfo.getInitialSplitSize() > 0) {
      SortedSet<Text> splits = Utils.getSortedSetFromFile(env, tableInfo.getSplitPath(), true);
      SortedSet<Text> dirs = Utils.getSortedSetFromFile(env, tableInfo.getSplitDirsPath(), false);
      Map<Text,Text> splitDirMap = createSplitDirectoryMap(splits, dirs);
      try (BatchWriter bw = env.getContext().createBatchWriter(MetadataTable.NAME)) {
        writeSplitsToMetadataTable(env.getContext(), tableInfo.getTableId(), splits, splitDirMap,
            tableInfo.getTimeType(), env.getManagerLock(), bw);
      }
    }
    return new FinishCreateTable(tableInfo);
  }

  private void writeSplitsToMetadataTable(ServerContext context, TableId tableId,
      SortedSet<Text> splits, Map<Text,Text> data, TimeType timeType, ServiceLock lock,
      BatchWriter bw) throws MutationsRejectedException {
    Text prevSplit = null;
    Value dirValue;
    Iterable<Text> iter = () -> Stream.concat(splits.stream(), Stream.of((Text) null)).iterator();
    for (Text split : iter) {
      Mutation mut =
          TabletColumnFamily.createPrevRowMutation(new KeyExtent(tableId, split, prevSplit));
      dirValue = (split == null) ? new Value(ServerColumnFamily.DEFAULT_TABLET_DIR_NAME)
          : new Value(data.get(split));
      ServerColumnFamily.DIRECTORY_COLUMN.put(mut, dirValue);
      ServerColumnFamily.TIME_COLUMN.put(mut, new Value(new MetadataTime(0, timeType).encode()));
      MetadataTableUtil.putLockID(context, lock, mut);
      prevSplit = split;
      bw.addMutation(mut);
    }
  }

  @Override
  public void undo(long tid, Manager environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.getTableId(), false, environment.getContext(),
        environment.getManagerLock());
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
