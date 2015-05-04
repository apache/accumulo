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
package org.apache.accumulo.test;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRandomDeletes {
  private static final Logger log = LoggerFactory.getLogger(TestRandomDeletes.class);
  private static Authorizations auths = new Authorizations("L1", "L2", "G1", "GROUP2");

  static private class RowColumn implements Comparable<RowColumn> {
    Text row;
    Column column;
    long timestamp;

    public RowColumn(Text row, Column column, long timestamp) {
      this.row = row;
      this.column = column;
      this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(row) + Objects.hashCode(column);
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj || (obj != null && obj instanceof RowColumn && 0 == compareTo((RowColumn) obj));
    }

    @Override
    public int compareTo(RowColumn other) {
      int result = row.compareTo(other.row);
      if (result != 0)
        return result;
      return column.compareTo(other.column);
    }

    @Override
    public String toString() {
      return row.toString() + ":" + column.toString();
    }
  }

  private static TreeSet<RowColumn> scanAll(ClientOnDefaultTable opts, ScannerOpts scanOpts, String tableName) throws Exception {
    TreeSet<RowColumn> result = new TreeSet<>();
    Connector conn = opts.getConnector();
    Scanner scanner = conn.createScanner(tableName, auths);
    scanner.setBatchSize(scanOpts.scanBatchSize);
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Column column = new Column(TextUtil.getBytes(key.getColumnFamily()), TextUtil.getBytes(key.getColumnQualifier()), TextUtil.getBytes(key
          .getColumnVisibility()));
      result.add(new RowColumn(key.getRow(), column, key.getTimestamp()));
    }
    return result;
  }

  private static long scrambleDeleteHalfAndCheck(ClientOnDefaultTable opts, ScannerOpts scanOpts, BatchWriterOpts bwOpts, String tableName, Set<RowColumn> rows)
      throws Exception {
    int result = 0;
    ArrayList<RowColumn> entries = new ArrayList<>(rows);
    java.util.Collections.shuffle(entries);

    Connector connector = opts.getConnector();
    BatchWriter mutations = connector.createBatchWriter(tableName, bwOpts.getBatchWriterConfig());

    for (int i = 0; i < (entries.size() + 1) / 2; i++) {
      RowColumn rc = entries.get(i);
      Mutation m = new Mutation(rc.row);
      m.putDelete(new Text(rc.column.columnFamily), new Text(rc.column.columnQualifier), new ColumnVisibility(rc.column.getColumnVisibility()),
          rc.timestamp + 1);
      mutations.addMutation(m);
      rows.remove(rc);
      result++;
    }

    mutations.close();

    Set<RowColumn> current = scanAll(opts, scanOpts, tableName);
    current.removeAll(rows);
    if (current.size() > 0) {
      throw new RuntimeException(current.size() + " records not deleted");
    }
    return result;
  }

  static public void main(String[] args) {

    ClientOnDefaultTable opts = new ClientOnDefaultTable("test_ingest");
    ScannerOpts scanOpts = new ScannerOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(TestRandomDeletes.class.getName(), args, scanOpts, bwOpts);

    log.info("starting random delete test");

    try {
      long deleted = 0;

      String tableName = opts.getTableName();

      TreeSet<RowColumn> doomed = scanAll(opts, scanOpts, tableName);
      log.info("Got {} rows", doomed.size());

      long startTime = System.currentTimeMillis();
      while (true) {
        long half = scrambleDeleteHalfAndCheck(opts, scanOpts, bwOpts, tableName, doomed);
        deleted += half;
        if (half == 0)
          break;
      }
      long stopTime = System.currentTimeMillis();

      long elapsed = (stopTime - startTime) / 1000;
      log.info("deleted {} values in {} seconds", deleted, elapsed);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
