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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.util.CheckForMetadataProblems;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class SplitIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "5K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "100ms");
    cfg.setSiteConfig(siteConfig);
  }
  
  @Test(timeout = 120 * 1000)
  public void tabletShouldSplit() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "256K");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.rows = 100000;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = opts.rows;
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    UtilWaitThread.sleep(15 * 1000);
    String id = c.tableOperations().tableIdMap().get("test_ingest");
    Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    KeyExtent extent = new KeyExtent(new Text(id), null, null);
    s.setRange(extent.toMetadataRange());
    MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
    int count = 0;
    int shortened = 0;
    for (Entry<Key,Value> entry : s) {
      extent = new KeyExtent(entry.getKey().getRow(), entry.getValue());
      if (extent.getEndRow() != null && extent.getEndRow().toString().length() < 14)
        shortened++;
      count++;
    }
    assertTrue(shortened > 0);
    assertTrue(count > 10);
    assertEquals(0,
        cluster.exec(CheckForMetadataProblems.class, "-i", cluster.getInstanceName(), "-u", "root", "-p", MacTest.PASSWORD, "-z", cluster.getZooKeepers())
            .waitFor());
  }
  
  @Test(timeout = 60 * 1000)
  public void interleaveSplit() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
    ReadWriteIT.interleaveTest(c);
    UtilWaitThread.sleep(5*1000);
    assertTrue(c.tableOperations().listSplits("test_ingest").size() > 20);
  }
  
  @Test(timeout = 120 * 1000)
  public void deleteSplit() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    DeleteIT.deleteTest(c, cluster);
    c.tableOperations().flush("test_ingest", null, null, true);
    UtilWaitThread.sleep(10*1000);
    assertTrue(c.tableOperations().listSplits("test_ingest").size() > 30);
  }
  
}
