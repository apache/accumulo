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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

/**
 * A functional test that exercises hitting the max open file limit on a tablet server. This test assumes there are one or two tablet servers.
 */

public class MaxOpenIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "4");
    conf.put(Property.TSERV_MAJC_MAXCONCURRENT.getKey(), "1");
    conf.put(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey(), "2");
    cfg.setSiteConfig(conf);
  }

  private static final int NUM_TABLETS = 16;
  private static final int NUM_TO_INGEST = 10000;
  
  @Test(timeout=30*1000)
  public void run() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_MAJC_RATIO.getKey(), "10");
    c.tableOperations().addSplits("test_ingest", TestIngest.getSplitPoints(0, NUM_TO_INGEST, NUM_TABLETS));
    
    // the following loop should create three tablets in each map file
    for (int i = 0; i < 3; i++) {
      TestIngest.Opts opts = new TestIngest.Opts();
      opts.timestamp = i;
      opts.dataSize = 50;
      opts.rows = NUM_TO_INGEST;
      opts.cols = 1;
      opts.random = i;
      TestIngest.ingest(c, opts, new BatchWriterOpts());
      
      c.tableOperations().flush("test_ingest", null, null, true);
      FunctionalTestUtils.checkRFiles(c, "test_ingest", NUM_TABLETS, NUM_TABLETS, i + 1, i + 1);
    }
    
    List<Range> ranges = new ArrayList<Range>(NUM_TO_INGEST);
    
    for (int i = 0; i < NUM_TO_INGEST; i++) {
      ranges.add(new Range(TestIngest.generateRow(i, 0)));
    }
    
    long time1 = batchScan(c, ranges, 1);
    // run it again, now that stuff is cached on the client and sever
    time1 = batchScan(c, ranges, 1);
    long time2 = batchScan(c, ranges, NUM_TABLETS);
    
    System.out.printf("Single thread scan time   %6.2f %n", time1 / 1000.0);
    System.out.printf("Multiple thread scan time %6.2f %n", time2 / 1000.0);
    
  }
  
  private long batchScan(Connector c, List<Range> ranges, int threads) throws Exception {
    BatchScanner bs = c.createBatchScanner("test_ingest", TestIngest.AUTHS, threads);
    
    bs.setRanges(ranges);
    
    int count = 0;
    
    long t1 = System.currentTimeMillis();
    
    byte rval[] = new byte[50];
    Random random = new Random();
    
    for (Entry<Key,Value> entry : bs) {
      count++;
      int row = VerifyIngest.getRow(entry.getKey());
      int col = VerifyIngest.getCol(entry.getKey());
      
      if (row < 0 || row >= NUM_TO_INGEST) {
        throw new Exception("unexcepted row " + row);
      }
      
      rval = TestIngest.genRandomValue(random, rval, 2, row, col);
      
      if (entry.getValue().compareTo(rval) != 0) {
        throw new Exception("unexcepted value row=" + row + " col=" + col);
      }
    }
    
    long t2 = System.currentTimeMillis();
    
    bs.close();
    
    if (count != NUM_TO_INGEST) {
      throw new Exception("Batch Scan did not return expected number of values " + count);
    }
    
    return t2 - t1;
  }
  
}
