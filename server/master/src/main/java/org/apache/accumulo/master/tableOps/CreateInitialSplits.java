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
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

public class CreateInitialSplits extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private TableInfo tableInfo;

  CreateInitialSplits(TableInfo ti) {
    this.tableInfo = ti;
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
  };

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {

    // Read table entry and store values for later use
    SplitEntry splitEntry = new SplitEntry();
    populateSplitEntry(environment, splitEntry);

    try (BatchWriter bw = environment.getConnector().createBatchWriter("accumulo.metadata")) {

      // Read splits from filesystem and write to metadata table.
      writeSplitsToMetadataTable(environment, tableInfo.splitFile, splitEntry, bw);

      // last row is handled as a special case
      writeLastRowToMetadataTable(splitEntry, bw);
    }

    return new FinishCreateTable(tableInfo);
  }

  private void writeLastRowToMetadataTable(SplitEntry splitEntry, BatchWriter bw)
      throws MutationsRejectedException {
    Text lastRow = new Text(tableInfo.tableId.toString() + "<");
    Mutation mut = new Mutation(lastRow);
    mut.put(new Text("srv"), new Text("dir"), splitEntry.dirValue);
    mut.put(new Text("srv"), new Text("lock"), splitEntry.lockValue);
    mut.put(new Text("srv"), new Text("time"), splitEntry.timeValue);
    mut.put(new Text("~tab"), new Text("~pr"),
        KeyExtent.encodePrevEndRow(new Text(splitEntry.lastSplit)));
    bw.addMutation(mut);
  }

  private void writeSplitsToMetadataTable(Master environment, String splitFile,
      SplitEntry splitEntry, BatchWriter bw) throws IOException {
    String tableId = tableInfo.tableId.toString();
    try (FSDataInputStream splitStream = environment.getInputStream(splitFile)) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(splitStream))) {
        String split;
        while ((split = br.readLine()) != null) {
          try {
            Mutation mut = new Mutation(tableId + ";" + split);
            mut.put(new Text("srv"), new Text("dir"), splitEntry.dirValue);
            mut.put(new Text("srv"), new Text("lock"), splitEntry.lockValue);
            mut.put(new Text("srv"), new Text("time"), splitEntry.timeValue);
            if (splitEntry.first) {
              mut.put(new Text("~tab"), new Text("~pr"), splitEntry.firstRow);
              splitEntry.first = false;
            } else {
              mut.put(new Text("~tab"), new Text("~pr"),
                  KeyExtent.encodePrevEndRow(new Text(splitEntry.lastSplit)));
            }
            bw.addMutation(mut);
            splitEntry.lastSplit = split;
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  /**
   * This method reads the newly created metadata table entry for the table being created and stores
   * the information into a SplitEntry class for use in follow-on methods.
   */
  private void populateSplitEntry(Master environment, SplitEntry splitEntry)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    String tableId = tableInfo.tableId.toString();
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
      if (colf.equals(new Text("~tab")))
        splitEntry.firstRow = new Value(entry.getValue());
    }
    scan.close();
  }

}
