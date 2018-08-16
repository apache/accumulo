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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

class PopulateMetadata extends MasterRepo {

  private static final Logger log = LoggerFactory.getLogger(PopulateMetadata.class);

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  PopulateMetadata(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {

    KeyExtent extent = new KeyExtent(tableInfo.tableId, null, null);
    MetadataTableUtil.addTablet(extent, tableInfo.dir, environment, tableInfo.timeType,
        environment.getMasterLock());

    if (tableInfo.initialSplitSize > 0) {
      SortedSet<Text> splits = Utils
          .getSortedSetFromFile(environment.getInputStream(tableInfo.splitFile), true);

      for (Text split : splits) {
        log.info(">>>> pmd.splits: "
            + Utils.getBytesAsString(TextUtil.getBytes(split), split.getLength()));
      }

      SortedSet<Text> dirs = Utils
          .getSortedSetFromFile(environment.getInputStream(tableInfo.splitDirsFile), false);
      for (Text dir : dirs) {
        log.info(">>>> pmd.dirs: " + dir);
      }

      Map<Text,Text> splitDirMap = createSplitDirectoryMap(splits, dirs);
      splitDirMap.forEach((k, v) -> log
          .info(">>>> pmd.map: " + Utils.getBytesAsString(TextUtil.getBytes(k)) + " : " + v));

      try (BatchWriter bw = environment.getConnector().createBatchWriter("accumulo.metadata")) {
        writeSplitsToMetadataTable(tableInfo.tableId, splits, splitDirMap, tableInfo.timeType,
            environment.getMasterLock(), bw);
      }
    }
    return new FinishCreateTable(tableInfo);
  }

  private void writeSplitsToMetadataTable(Table.ID tableId, SortedSet<Text> splits,
      Map<Text,Text> data, char timeType, ZooLock lock, BatchWriter bw)
      throws MutationsRejectedException {

    Text prevSplit = null;
    for (Text split : Iterables.concat(splits, Collections.singleton(null))) {
      Mutation mut = new KeyExtent(tableId, split, prevSplit).getPrevRowUpdateMutation();
      if (split == null) {
        MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut,
            new Value(tableInfo.dir));
      } else {
        MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut,
            new Value(data.get(split)));
      }
      MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut,
          new Value(timeType + "0"));
      MetadataTableUtil.putLockID(lock, mut);
      prevSplit = split;
      bw.addMutation(mut);
    }
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.tableId, false, environment,
        environment.getMasterLock());
  }

  private Map<Text,Text> createSplitDirectoryMap(SortedSet<Text> splits, SortedSet<Text> dirs) {
    Map<Text,Text> data = new HashMap<>();
    Iterator<Text> s = splits.iterator();
    Iterator<Text> d = dirs.iterator();
    while (s.hasNext() && d.hasNext()) {
      data.put(s.next(), d.next());
    }
    return data;
  }
}
