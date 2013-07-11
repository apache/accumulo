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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.server.gc.SimpleGarbageCollector;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class GarbageCollectorIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String,String> settings = new HashMap<String,String>();
    settings.put(Property.GC_CYCLE_START.getKey(), "1");
    settings.put(Property.GC_CYCLE_DELAY.getKey(), "1");
    settings.put(Property.TSERV_MAXMEM.getKey(), "5K");
    settings.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    cfg.setSiteConfig(settings);
  }
  
  @Test(timeout = 2 * 60 * 1000)
  public void gcTest() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "5K");
    TestIngest.Opts opts = new TestIngest.Opts();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = opts.rows = 10000;
    vopts.cols = opts.cols = 1;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    c.tableOperations().compact("test_ingest", null, null, true, true);
    int before = countFiles();
    while (true) {
      UtilWaitThread.sleep(1000);
      int more = countFiles();
      if (more <= before)
        break;
      before = more;
    }
    Process gc = cluster.exec(SimpleGarbageCollector.class);
    UtilWaitThread.sleep(5 * 1000);
    int after = countFiles();
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    assertTrue(after < before);
    gc.destroy();
  }
  
  @Test(timeout = 2 * 60 * 1000)
  public void gcLotsOfCandidatesIT() throws Exception {
    log.info("Filling !METADATA table with bogus delete flags");
    Connector c = getConnector();
    addEntries(c, new BatchWriterOpts());
    cluster.getConfig().setDefaultMemory(10, MemoryUnit.MEGABYTE);
    Process gc = cluster.exec(SimpleGarbageCollector.class);
    UtilWaitThread.sleep(10 * 1000);
    String output = FunctionalTestUtils.readAll(cluster, SimpleGarbageCollector.class, gc);
    gc.destroy();
    assertTrue(output.contains("delete candidates has exceeded"));
  }
  
  private int countFiles() throws Exception {
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    int result = 0;
    Path path = new Path(cluster.getConfig().getDir() + "/accumulo/tables/1/*/*.rf");
    for (@SuppressWarnings("unused")
    FileStatus entry : fs.globStatus(path)) {
      result++;
    }
    return result;
  }
  
  public static void addEntries(Connector conn, BatchWriterOpts bwOpts) throws Exception {
    conn.securityOperations().grantTablePermission(conn.whoami(), MetadataTable.NAME, TablePermission.WRITE);
    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, bwOpts.getBatchWriterConfig());
    
    for (int i = 0; i < 100000; ++i) {
      final Text emptyText = new Text("");
      Text row = new Text(String.format("%s%s%020d%s", MetadataSchema.DeletesSection.getRowPrefix(), "/", i,
          "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj"));
      Mutation delFlag = new Mutation(row);
      delFlag.put(emptyText, emptyText, new Value(new byte[] {}));
      bw.addMutation(delFlag);
    }
    bw.close();
  }
}
