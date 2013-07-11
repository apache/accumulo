package org.apache.accumulo.test.functional;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TableIT extends MacTest {
  
  @Test(timeout = 2 * 60 * 1000)
  public void test() throws Exception {
    Connector c = getConnector();
    TableOperations to = c.tableOperations();
    to.create("test_ingest");
    TestIngest.Opts opts = new TestIngest.Opts();
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    to.flush("test_ingest", null, null, true);
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    String id = to.tableIdMap().get("test_ingest");
    Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.setRange(new KeyExtent(new Text(id), null, null).toMetadataRange());
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : s) {
      count++;
    }
    assertTrue(count > 0);
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    assertTrue(fs.listStatus(new Path(cluster.getConfig().getDir() + "/accumulo/tables/" + id)).length > 0);
    to.delete("test_ingest");
    count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : s) {
      count++;
    }
    assertEquals(0, count);
    assertEquals(0, fs.listStatus(new Path(cluster.getConfig().getDir() + "/accumulo/tables/" + id)).length);
    assertNull(to.tableIdMap().get("test_ingest"));
    to.create("test_ingest");
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    to.delete("test_ingest");
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
  }
  
}
