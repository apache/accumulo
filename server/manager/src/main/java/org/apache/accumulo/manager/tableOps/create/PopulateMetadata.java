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
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
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
  public long isReady(FateId fateId, Manager environment) {
    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager env) throws Exception {
    SortedSet<Text> splits;
    Map<Text,Text> splitDirMap;

    if (tableInfo.getInitialSplitSize() > 0) {
      splits = Utils.getSortedSetFromFile(env, tableInfo.getSplitPath(), true);
      SortedSet<Text> dirs = Utils.getSortedSetFromFile(env, tableInfo.getSplitDirsPath(), false);
      splitDirMap = createSplitDirectoryMap(splits, dirs);
    } else {
      splits = new TreeSet<>();
      splitDirMap = Map.of();
    }

    writeSplitsToMetadataTable(env.getContext(), splits, splitDirMap, env.getManagerLock());

    return new FinishCreateTable(tableInfo);
  }

  private void writeSplitsToMetadataTable(ServerContext context, SortedSet<Text> splits,
      Map<Text,Text> data, ServiceLock lock) {
    try (var tabletsMutator = context.getAmple().mutateTablets()) {
      Text prevSplit = null;
      Iterable<Text> iter = () -> Stream.concat(splits.stream(), Stream.of((Text) null)).iterator();
      for (Text split : iter) {
        var extent = new KeyExtent(tableInfo.getTableId(), split, prevSplit);

        var tabletMutator = tabletsMutator.mutateTablet(extent);

        String dirName = (split == null) ? ServerColumnFamily.DEFAULT_TABLET_DIR_NAME
            : data.get(split).toString();

        tabletMutator.putPrevEndRow(extent.prevEndRow());
        tabletMutator.putDirName(dirName);
        tabletMutator.putTime(new MetadataTime(0, tableInfo.getTimeType()));
        tabletMutator.putZooLock(context.getZooKeeperRoot(), lock);
        tabletMutator.putTabletAvailability(tableInfo.getInitialTabletAvailability());
        tabletMutator.mutate();

        prevSplit = split;
      }
    }
  }

  @Override
  public void undo(FateId fateId, Manager environment) throws Exception {
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
