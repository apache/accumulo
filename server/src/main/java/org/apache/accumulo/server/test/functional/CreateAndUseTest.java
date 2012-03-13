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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

public class CreateAndUseTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {
    
  }
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.emptyList();
  }
  
  @Override
  public void run() throws Exception {
    SortedSet<Text> splits = new TreeSet<Text>();
    
    for (int i = 1; i < 256; i++) {
      splits.add(new Text(String.format("%08x", i << 8)));
    }
    
    // TEST 1 create a table and immediately batch write to it
    
    Text cf = new Text("cf1");
    Text cq = new Text("cq1");
    
    getConnector().tableOperations().create("t1");
    getConnector().tableOperations().addSplits("t1", splits);
    BatchWriter bw = getConnector().createBatchWriter("t1", 100000, Long.MAX_VALUE, 3);
    
    for (int i = 1; i < 257; i++) {
      Mutation m = new Mutation(new Text(String.format("%08x", (i << 8) - 16)));
      m.put(cf, cq, new Value(("" + i).getBytes()));
      
      bw.addMutation(m);
    }
    
    bw.close();
    
    // verify data is there
    Scanner scanner1 = getConnector().createScanner("t1", Constants.NO_AUTHS);
    
    int ei = 1;
    
    for (Entry<Key,Value> entry : scanner1) {
      if (!entry.getKey().getRow().toString().equals(String.format("%08x", (ei << 8) - 16))) {
        throw new Exception("Expected row " + String.format("%08x", (ei << 8) - 16) + " saw " + entry.getKey().getRow());
      }
      
      if (!entry.getValue().toString().equals("" + ei)) {
        throw new Exception("Expected value " + ei + " saw " + entry.getValue());
      }
      
      ei++;
    }
    
    if (ei != 257) {
      throw new Exception("Did not see expected number of rows, ei = " + ei);
    }
    
    // TEST 2 create a table and immediately scan it
    getConnector().tableOperations().create("t2");
    getConnector().tableOperations().addSplits("t2", splits);
    Scanner scanner2 = getConnector().createScanner("t2", Constants.NO_AUTHS);
    int count = 0;
    for (Entry<Key,Value> entry : scanner2) {
      if (entry != null)
        count++;
    }
    
    if (count != 0) {
      throw new Exception("Did not see expected number of entries, count = " + count);
    }
    
    // TEST 3 create a table and immediately batch scan it
    
    ArrayList<Range> ranges = new ArrayList<Range>();
    for (int i = 1; i < 257; i++) {
      ranges.add(new Range(new Text(String.format("%08x", (i << 8) - 16))));
    }
    
    getConnector().tableOperations().create("t3");
    getConnector().tableOperations().addSplits("t3", splits);
    BatchScanner bs = getConnector().createBatchScanner("t3", Constants.NO_AUTHS, 3);
    bs.setRanges(ranges);
    count = 0;
    for (Entry<Key,Value> entry : bs) {
      if (entry != null)
        count++;
    }
    
    if (count != 0) {
      throw new Exception("Did not see expected number of entries, count = " + count);
    }
    
    bs.close();
    
  }
  
}
