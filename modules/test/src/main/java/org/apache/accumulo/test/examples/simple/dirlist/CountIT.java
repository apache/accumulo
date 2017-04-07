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
package org.apache.accumulo.test.examples.simple.dirlist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.examples.simple.dirlist.FileCount;
import org.apache.accumulo.examples.simple.dirlist.FileCount.Opts;
import org.apache.accumulo.examples.simple.dirlist.Ingest;
import org.apache.accumulo.examples.simple.dirlist.QueryUtil;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class CountIT extends ConfigurableMacBase {

  private Connector conn;
  private String tableName;

  @Before
  public void setupInstance() throws Exception {
    tableName = getUniqueNames(1)[0];
    conn = getConnector();
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    ColumnVisibility cv = new ColumnVisibility();
    // / has 1 dir
    // /local has 2 dirs 1 file
    // /local/user1 has 2 files
    bw.addMutation(Ingest.buildMutation(cv, "/local", true, false, true, 272, 12345, null));
    bw.addMutation(Ingest.buildMutation(cv, "/local/user1", true, false, true, 272, 12345, null));
    bw.addMutation(Ingest.buildMutation(cv, "/local/user2", true, false, true, 272, 12345, null));
    bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 12345, null));
    bw.addMutation(Ingest.buildMutation(cv, "/local/file", false, false, false, 1024, 23456, null));
    bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file1", false, false, false, 2024, 12345, null));
    bw.addMutation(Ingest.buildMutation(cv, "/local/user1/file2", false, false, false, 1028, 23456, null));
    bw.close();
  }

  @Test
  public void test() throws Exception {
    Scanner scanner = conn.createScanner(tableName, new Authorizations());
    scanner.fetchColumn(new Text("dir"), new Text("counts"));
    assertFalse(scanner.iterator().hasNext());

    Opts opts = new Opts();
    ScannerOpts scanOpts = new ScannerOpts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.instance = conn.getInstance().getInstanceName();
    opts.zookeepers = conn.getInstance().getZooKeepers();
    opts.setTableName(tableName);
    opts.setPrincipal(conn.whoami());
    opts.setPassword(new Opts.Password(ROOT_PASSWORD));
    FileCount fc = new FileCount(opts, scanOpts, bwOpts);
    fc.run();

    ArrayList<Pair<String,String>> expected = new ArrayList<>();
    expected.add(new Pair<>(QueryUtil.getRow("").toString(), "1,0,3,3"));
    expected.add(new Pair<>(QueryUtil.getRow("/local").toString(), "2,1,2,3"));
    expected.add(new Pair<>(QueryUtil.getRow("/local/user1").toString(), "0,2,0,2"));
    expected.add(new Pair<>(QueryUtil.getRow("/local/user2").toString(), "0,0,0,0"));

    int i = 0;
    for (Entry<Key,Value> e : scanner) {
      assertEquals(e.getKey().getRow().toString(), expected.get(i).getFirst());
      assertEquals(e.getValue().toString(), expected.get(i).getSecond());
      i++;
    }
    assertEquals(i, expected.size());
  }
}
