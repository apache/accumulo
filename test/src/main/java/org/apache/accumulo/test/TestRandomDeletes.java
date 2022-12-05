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
package org.apache.accumulo.test;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
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

import com.beust.jcommander.Parameter;

public class TestRandomDeletes {
  private static final Logger log = LoggerFactory.getLogger(TestRandomDeletes.class);
  private static Authorizations auths = new Authorizations("L1", "L2", "G1", "GROUP2");

  private static class RowColumn implements Comparable<RowColumn> {
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
      return this == obj
          || (obj != null && obj instanceof RowColumn && compareTo((RowColumn) obj) == 0);
    }

    @Override
    public int compareTo(RowColumn other) {
      int result = row.compareTo(other.row);
      if (result != 0) {
        return result;
      }
      return column.compareTo(other.column);
    }

    @Override
    public String toString() {
      return row + ":" + column;
    }
  }

  private static TreeSet<RowColumn> scanAll(TestOpts opts) throws Exception {
    TreeSet<RowColumn> result = new TreeSet<>();
    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build();
        Scanner scanner = client.createScanner(opts.tableName, auths)) {
      for (Entry<Key,Value> entry : scanner) {
        Key key = entry.getKey();
        Column column = new Column(TextUtil.getBytes(key.getColumnFamily()),
            TextUtil.getBytes(key.getColumnQualifier()),
            TextUtil.getBytes(key.getColumnVisibility()));
        result.add(new RowColumn(key.getRow(), column, key.getTimestamp()));
      }
    }
    return result;
  }

  private static long scrambleDeleteHalfAndCheck(TestOpts opts, Set<RowColumn> rows)
      throws Exception {
    int result = 0;
    ArrayList<RowColumn> entries = new ArrayList<>(rows);
    java.util.Collections.shuffle(entries);

    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build();
        BatchWriter bw = client.createBatchWriter(opts.tableName)) {

      for (int i = 0; i < (entries.size() + 1) / 2; i++) {
        RowColumn rc = entries.get(i);
        Mutation m = new Mutation(rc.row);
        m.putDelete(new Text(rc.column.columnFamily), new Text(rc.column.columnQualifier),
            new ColumnVisibility(rc.column.getColumnVisibility()), rc.timestamp + 1);
        bw.addMutation(m);
        rows.remove(rc);
        result++;
      }
    }

    Set<RowColumn> current = scanAll(opts);
    current.removeAll(rows);
    if (!current.isEmpty()) {
      throw new RuntimeException(current.size() + " records not deleted");
    }
    return result;
  }

  static class TestOpts extends ClientOpts {
    @Parameter(names = "--table", description = "table to use")
    String tableName = "test_ingest";
  }

  public static void main(String[] args) {

    TestOpts opts = new TestOpts();
    opts.parseArgs(TestRandomDeletes.class.getName(), args);

    log.info("starting random delete test");

    try {
      long deleted = 0;

      TreeSet<RowColumn> doomed = scanAll(opts);
      log.info("Got {} rows", doomed.size());

      long startTime = System.currentTimeMillis();
      while (true) {
        long half = scrambleDeleteHalfAndCheck(opts, doomed);
        deleted += half;
        if (half == 0) {
          break;
        }
      }
      long stopTime = System.currentTimeMillis();

      long elapsed = (stopTime - startTime) / 1000;
      log.info("deleted {} values in {} seconds", deleted, elapsed);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
