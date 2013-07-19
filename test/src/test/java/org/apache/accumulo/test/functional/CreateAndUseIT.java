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
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class CreateAndUseIT extends SimpleMacIT {
  
  @Test(timeout=60*1000)
  public void run() throws Exception {
    SortedSet<Text> splits = new TreeSet<Text>();
    
    for (int i = 1; i < 256; i++) {
      splits.add(new Text(String.format("%08x", i << 8)));
    }
    
    // TEST 1 create a table and immediately batch write to it
    
    Text cf = new Text("cf1");
    Text cq = new Text("cq1");
    
    String tableName = makeTableName();
    getConnector().tableOperations().create(tableName);
    getConnector().tableOperations().addSplits(tableName, splits);
    BatchWriter bw = getConnector().createBatchWriter(tableName, new BatchWriterConfig());
    
    for (int i = 1; i < 257; i++) {
      Mutation m = new Mutation(new Text(String.format("%08x", (i << 8) - 16)));
      m.put(cf, cq, new Value(("" + i).getBytes()));
      
      bw.addMutation(m);
    }
    
    bw.close();
    
    // verify data is there
    Scanner scanner1 = getConnector().createScanner(tableName, Authorizations.EMPTY);
    
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
    String table2 = makeTableName();
    getConnector().tableOperations().create(table2);
    getConnector().tableOperations().addSplits(table2, splits);
    Scanner scanner2 = getConnector().createScanner(table2, Authorizations.EMPTY);
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

    String table3 = makeTableName();
    getConnector().tableOperations().create(table3);
    getConnector().tableOperations().addSplits(table3, splits);
    BatchScanner bs = getConnector().createBatchScanner(table3, Authorizations.EMPTY, 3);
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
