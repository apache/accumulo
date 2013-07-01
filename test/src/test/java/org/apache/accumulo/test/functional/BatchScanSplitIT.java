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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BatchScanSplitIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setSiteConfig(Collections.singletonMap(Property.TSERV_MAJC_DELAY.getKey(), "0"));
  }
  
  @Test(timeout=60*1000)
  public void test() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("bss");
    
    int numRows = 1 << 18;
    
    BatchWriter bw = getConnector().createBatchWriter("bss", new BatchWriterConfig());
    
    for (int i = 0; i < numRows; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value(String.format("%016x", numRows - i).getBytes()));
      bw.addMutation(m);
    }
    
    bw.close();
    
    getConnector().tableOperations().flush("bss", null, null, true);
    
    getConnector().tableOperations().setProperty("bss", Property.TABLE_SPLIT_THRESHOLD.getKey(), "4K");
    
    Collection<Text> splits = getConnector().tableOperations().listSplits("bss");
    while (splits.size() < 2) {
      UtilWaitThread.sleep(1);
      splits = getConnector().tableOperations().listSplits("bss");
    }
    
    System.out.println("splits : " + splits);
    
    Random random = new Random(19011230);
    HashMap<Text,Value> expected = new HashMap<Text,Value>();
    ArrayList<Range> ranges = new ArrayList<Range>();
    for (int i = 0; i < 100; i++) {
      int r = random.nextInt(numRows);
      Text row = new Text(String.format("%09x", r));
      expected.put(row, new Value(String.format("%016x", numRows - r).getBytes()));
      ranges.add(new Range(row));
    }
    
    // logger.setLevel(Level.TRACE);
    
    HashMap<Text,Value> found = new HashMap<Text,Value>();
    
    for (int i = 0; i < 20; i++) {
      BatchScanner bs = getConnector().createBatchScanner("bss", Authorizations.EMPTY, 4);
      
      found.clear();
      
      long t1 = System.currentTimeMillis();
      
      bs.setRanges(ranges);
      
      for (Entry<Key,Value> entry : bs) {
        found.put(entry.getKey().getRow(), entry.getValue());
      }
      bs.close();
      
      long t2 = System.currentTimeMillis();
      
      log.info(String.format("rate : %06.2f%n", ranges.size() / ((t2 - t1) / 1000.0)));
      
      if (!found.equals(expected))
        throw new Exception("Found and expected differ " + found + " " + expected);
    }
    
    splits = getConnector().tableOperations().listSplits("bss");
    log.info("splits : " + splits);
  }
  
}
