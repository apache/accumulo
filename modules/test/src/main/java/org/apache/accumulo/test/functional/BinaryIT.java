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
package org.apache.accumulo.test.functional;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestBinaryRows;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BinaryIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 90;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    runTest(c, tableName);
  }

  @Test
  public void testPreSplit() throws Exception {
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(tableName);
    SortedSet<Text> splits = new TreeSet<>();
    splits.add(new Text("8"));
    splits.add(new Text("256"));
    c.tableOperations().addSplits(tableName, splits);
    runTest(c, tableName);
  }

  public static void runTest(Connector c, String tableName) throws Exception {
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    ScannerOpts scanOpts = new ScannerOpts();
    TestBinaryRows.Opts opts = new TestBinaryRows.Opts();
    opts.setTableName(tableName);
    opts.start = 0;
    opts.num = 100000;
    opts.mode = "ingest";
    TestBinaryRows.runTest(c, opts, bwOpts, scanOpts);
    opts.mode = "verify";
    TestBinaryRows.runTest(c, opts, bwOpts, scanOpts);
    opts.start = 25000;
    opts.num = 50000;
    opts.mode = "delete";
    TestBinaryRows.runTest(c, opts, bwOpts, scanOpts);
    opts.start = 0;
    opts.num = 25000;
    opts.mode = "verify";
    TestBinaryRows.runTest(c, opts, bwOpts, scanOpts);
    opts.start = 75000;
    opts.num = 25000;
    opts.mode = "randomLookups";
    TestBinaryRows.runTest(c, opts, bwOpts, scanOpts);
    opts.start = 25000;
    opts.num = 50000;
    opts.mode = "verifyDeleted";
    TestBinaryRows.runTest(c, opts, bwOpts, scanOpts);
  }

}
