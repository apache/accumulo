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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

public class CompactionIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String,String> map = new HashMap<String,String>();
    map.put(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "4");
    map.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    map.put(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "1");
    cfg.setSiteConfig(map);
  }
  
  @Test(timeout = 120 * 1000)
  public void test() throws Exception {
    final Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_MAJC_RATIO.getKey(), "1.0");
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    FunctionalTestUtils.createRFiles(c, fs, "tmp/testrf", 500000, 59, 4);
    FunctionalTestUtils.bulkImport(c, fs, "test_ingest", "tmp/testrf");
    int beforeCount = countFiles(c);
    
    final AtomicBoolean fail = new AtomicBoolean(false);
    for (int count = 0; count < 5; count++) {
      List<Thread> threads = new ArrayList<Thread>();
      final int span = 500000 / 59;
      for (int i = 0; i < 500000; i += 500000 / 59) {
        final int finalI = i;
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              VerifyIngest.Opts opts = new VerifyIngest.Opts();
              opts.startRow = finalI;
              opts.rows = span;
              opts.random = 56;
              opts.dataSize = 50;
              opts.cols = 1;
              VerifyIngest.verifyIngest(c, opts, new ScannerOpts());
            } catch (Exception ex) {
              fail.set(true);
            }
          }
        };
        t.start();
        threads.add(t);
      }
      for (Thread t : threads)
        t.join();
      assertFalse(fail.get());
    }
    
    int finalCount = countFiles(c);
    assertTrue(finalCount < beforeCount);
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
  }
  
  private int countFiles(Connector c) throws Exception {
    Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.fetchColumnFamily(MetadataSchema.TabletsSection.TabletColumnFamily.NAME);
    s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    int i = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : s)
      i++;
    return i;
  }
  
}
