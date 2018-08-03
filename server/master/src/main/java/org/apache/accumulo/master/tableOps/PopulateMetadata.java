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

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

class PopulateMetadata extends MasterRepo {

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

    if (tableInfo.props.containsKey(Property.TABLE_OFFLINE_OPTS + "create.initial.splits")) {
      SortedSet<Text> splits = readSplits(tableInfo.splitFile, environment);
      try (BatchWriter bw = environment.getConnector().createBatchWriter("accumulo.metadata")) {
        writeSplitsToMetadataTable(tableInfo.tableId, splits, tableInfo.timeType,
            environment.getMasterLock(), bw);
      }
    }
    return new FinishCreateTable(tableInfo);
  }

  private void writeSplitsToMetadataTable(Table.ID tableId, SortedSet<Text> splits, char timeType, ZooLock lock, BatchWriter bw) throws MutationsRejectedException {
    Text prevSplit = null;
    for (Text split : Iterables.concat(splits, Collections.singleton(null))) {
      Mutation mut = new KeyExtent(tableId, split, prevSplit).getPrevRowUpdateMutation();
      MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut,
          new Value(tableInfo.dir));
      MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut, new Value(timeType+"0"));
      MetadataTableUtil.putLockID(lock, mut);
      prevSplit = split;
      bw.addMutation(mut);
    }
  }

  /*
      Function<Text,Value> dirFunction,
      ServerColumnFamily.DIRECTORY_COLUMN.put(mut, dirFunction.apply(split));
   */

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.tableId, false, environment,
        environment.getMasterLock());
  }

  private SortedSet<Text> readSplits(String splitFile, Master environment) throws IOException {
    SortedSet<Text> splits;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(environment.getInputStream(splitFile)))) {
      splits = br.lines().map(Text::new)
          .collect(Collectors.toCollection(TreeSet::new));
    }
    return splits;
  }
}
