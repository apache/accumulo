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
package org.apache.accumulo.server.test.functional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

public class BatchScanSplitTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    HashMap<String,String> conf = new HashMap<String,String>();
    conf.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    return conf;
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.singletonList(new TableSetup("bss"));
  }
  
  @Override
  public void run() throws Exception {
    
    // Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    
    int numRows = 1 << 18;
    
    BatchWriter bw = getConnector().createBatchWriter("bss", 10000000, 60000l, 3);
    
    for (int i = 0; i < numRows; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value(String.format("%016x", numRows - i).getBytes()));
      bw.addMutation(m);
    }
    
    bw.close();
    
    getConnector().tableOperations().flush("bss", null, null, true);
    
    getConnector().tableOperations().setProperty("bss", Property.TABLE_SPLIT_THRESHOLD.getKey(), "4K");
    
    Collection<Text> splits = getConnector().tableOperations().getSplits("bss");
    while (splits.size() < 2) {
      UtilWaitThread.sleep(1);
      splits = getConnector().tableOperations().getSplits("bss");
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
    
    BatchScanner bs = getConnector().createBatchScanner("bss", Constants.NO_AUTHS, 4);
    
    HashMap<Text,Value> found = new HashMap<Text,Value>();
    
    for (int i = 0; i < 20; i++) {
      
      found.clear();
      
      long t1 = System.currentTimeMillis();
      
      bs.setRanges(ranges);
      
      for (Entry<Key,Value> entry : bs) {
        found.put(entry.getKey().getRow(), entry.getValue());
      }
      
      long t2 = System.currentTimeMillis();
      
      System.out.printf("rate : %06.2f\n", ranges.size() / ((t2 - t1) / 1000.0));
      
      if (!found.equals(expected))
        throw new Exception("Found and expected differ " + found + " " + expected);
    }
    
    splits = getConnector().tableOperations().getSplits("bss");
    System.out.println("splits : " + splits);
    
  }
  
}
