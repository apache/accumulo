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
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.test.TestIngest;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * See ACCUMULO-779
 */
public class FateStarvationIT extends MacTest {
  
  @Test(timeout=2 * 60 * 1000)
  public void run() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    
    c.tableOperations().addSplits("test_ingest", TestIngest.getSplitPoints(0, 100000, 50));
    
    TestIngest.Opts opts = new TestIngest.Opts();
    opts.random = 89;
    opts.timestamp = 7;
    opts.dataSize = 50;
    opts.rows = 100000;
    opts.cols = 1;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    
    c.tableOperations().flush("test_ingest", null, null, true);
    
    List<Text> splits = new ArrayList<Text>(TestIngest.getSplitPoints(0, 100000, 67));
    Random rand = new Random();
    
    for (int i = 0; i < 100; i++) {
      int idx1 = rand.nextInt(splits.size() - 1);
      int idx2 = rand.nextInt(splits.size() - (idx1 + 1)) + idx1 + 1;
      
      c.tableOperations().compact("test_ingest", splits.get(idx1), splits.get(idx2), false, false);
    }
    
    c.tableOperations().offline("test_ingest");
  }
  
}
