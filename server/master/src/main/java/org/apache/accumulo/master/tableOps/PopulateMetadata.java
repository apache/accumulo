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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PopulateMetadata extends MasterRepo {

  public static final Logger log = LoggerFactory.getLogger(PopulateMetadata.class);

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  PopulateMetadata(TableInfo ti) {
    log.info(">>>> PopulateMetadata constructor...");
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {

    log.info(">>>> PopulateMetadata:call...");

    KeyExtent extent = new KeyExtent(tableInfo.tableId, null, null);
    MetadataTableUtil.addTablet(extent, tableInfo.dir, environment, tableInfo.timeType,
        environment.getMasterLock());

    if (tableInfo.props.containsKey(Property.TABLE_OFFLINE_OPTS + "create.initial.splits")) {
      log.info(">>>> create initial splits");

      SplitEntry splitEntry = new SplitEntry();
      populateSplitEntry(environment, splitEntry);
      log.info(">>>> completed populating SplitEntry...");

      try (BatchWriter bw = environment.getConnector().createBatchWriter("accumulo.metadata")) {

        // Read splits from filesystem and write to metadata table.
        writeSplitsToMetadataTable(environment, tableInfo.splitFile, splitEntry, bw);

        // last row is handled as a special case
        writeLastRowToMetadataTable(splitEntry, bw);
      }

    }

    return new FinishCreateTable(tableInfo);
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {
    MetadataTableUtil.deleteTable(tableInfo.tableId, false, environment,
        environment.getMasterLock());
  }

  private void writeSplitsToMetadataTable2(Table.ID tableId, SortedSet<Text> splits,
      Function<Text,Value> dirFunction, SplitEntry splitEntry, char timeType, ZooLock lock,
      BatchWriter bw) throws MutationsRejectedException {
    Text prevSplit = null;
    for (Text split : Iterables.concat(splits, Collections.singleton(null))) {
      Mutation mut = new KeyExtent(tableId, split, prevSplit).getPrevRowUpdateMutation();
      MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut,
          splitEntry.dirValue);
      // MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut,
      // dirFunction.apply(split));
      MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut,
          new Value(timeType + "0"));
      MetadataTableUtil.putLockID(lock, mut);
      prevSplit = split;

      bw.addMutation(mut);
    }
  }

  private class SplitEntry {
    Value dirValue;
    Value lockValue;
    Value timeValue;
    Value firstRow;
    String lastSplit;
    boolean first;

    public SplitEntry() {
      dirValue = null;
      lockValue = null;
      timeValue = null;
      firstRow = null;
      lastSplit = null;
      first = true;
    }
  }

  ;

  /**
   * This method reads the newly created metadata table entry for the table being created and stores
   * the information into a SplitEntry class for use in follow-on methods.
   */
  private void populateSplitEntry(Master environment, SplitEntry splitEntry)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    String tableId = tableInfo.tableId.canonicalID();
    Scanner scan = environment.getConnector().createScanner("accumulo.metadata",
        Authorizations.EMPTY);
    scan.setRange(new Range(tableId + "<"));
    for (Map.Entry<Key,Value> entry : scan) {
      Text colf = new Text(entry.getKey().getColumnFamily());
      Text colq = new Text(entry.getKey().getColumnQualifier());
      if (colf.equals(new Text("srv"))) {
        switch (colq.toString()) {
          case "dir":
            splitEntry.dirValue = new Value(entry.getValue());
            break;
          case "lock":
            splitEntry.lockValue = new Value(entry.getValue());
            break;
          case "time":
            splitEntry.timeValue = new Value(entry.getValue());
            break;
        }
      }
      if (colf.equals(new Text(MetadataSchema.TabletsSection.TabletColumnFamily.STR_NAME))) {
        splitEntry.firstRow = new Value(entry.getValue());
      }
    }
    scan.close();
  }

  private void writeLastRowToMetadataTable(SplitEntry splitEntry, BatchWriter bw)
      throws MutationsRejectedException {
    Text lastRow = new Text(tableInfo.tableId.toString() + "<");
    Mutation mut = new Mutation(lastRow);
    MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut, splitEntry.dirValue);
    MetadataSchema.TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(mut, splitEntry.lockValue);
    MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut, splitEntry.timeValue);
    MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(mut,
        KeyExtent.encodePrevEndRow(new Text(splitEntry.lastSplit)));
    bw.addMutation(mut);
  }

  private void writeSplitsToMetadataTable(Master environment, String splitFile,
      SplitEntry splitEntry, BatchWriter bw) throws IOException, MutationsRejectedException {
    String tableId = tableInfo.tableId.toString();
    try (FSDataInputStream splitStream = environment.getInputStream(splitFile)) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(splitStream))) {
        String split;
        while ((split = br.readLine()) != null) {
          Mutation mut = new Mutation(tableId + ";" + split);
          MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(mut,
              splitEntry.dirValue);
          // ServerColumnFamily.DIRECTORY_COLUMN.put(mut, );
          MetadataSchema.TabletsSection.ServerColumnFamily.LOCK_COLUMN.put(mut,
              splitEntry.lockValue);
          MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.put(mut,
              splitEntry.timeValue);
          if (splitEntry.first) {
            MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(mut,
                splitEntry.firstRow);
            splitEntry.first = false;
          } else {
            MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(mut,
                KeyExtent.encodePrevEndRow(new Text(splitEntry.lastSplit)));
          }
          bw.addMutation(mut);
          splitEntry.lastSplit = split;
        }
      }
    }
  }

}
