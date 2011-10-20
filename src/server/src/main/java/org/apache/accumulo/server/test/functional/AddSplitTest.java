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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

public class AddSplitTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.singletonList(new TableSetup("foo"));
  }
  
  @Override
  public void run() throws Exception {
    
    // Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME+".client");
    // logger.setLevel(Level.TRACE);
    
    insertData(1l);
    
    TreeSet<Text> splits = new TreeSet<Text>();
    splits.add(new Text(String.format("%09d", 333)));
    splits.add(new Text(String.format("%09d", 666)));
    
    getConnector().tableOperations().addSplits("foo", splits);
    
    UtilWaitThread.sleep(100);
    
    Collection<Text> actualSplits = getConnector().tableOperations().getSplits("foo");
    
    if (!splits.equals(new TreeSet<Text>(actualSplits))) {
      throw new Exception(splits + " != " + actualSplits);
    }
    
    verifyData(1l);
    insertData(2l);
    
    // did not clear splits on purpose, it should ignore existing split points
    // and still create the three additional split points
    
    splits.add(new Text(String.format("%09d", 200)));
    splits.add(new Text(String.format("%09d", 500)));
    splits.add(new Text(String.format("%09d", 800)));
    
    getConnector().tableOperations().addSplits("foo", splits);
    
    UtilWaitThread.sleep(100);
    
    actualSplits = getConnector().tableOperations().getSplits("foo");
    
    if (!splits.equals(new TreeSet<Text>(actualSplits))) {
      throw new Exception(splits + " != " + actualSplits);
    }
    
    verifyData(2l);
  }
  
  private void verifyData(long ts) throws Exception {
    Scanner scanner = getConnector().createScanner("foo", Constants.NO_AUTHS);
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    
    for (int i = 0; i < 10000; i++) {
      if (!iter.hasNext()) {
        throw new Exception("row " + i + " not found");
      }
      
      Entry<Key,Value> entry = iter.next();
      
      String row = String.format("%09d", i);
      
      if (!entry.getKey().getRow().equals(new Text(row))) {
        throw new Exception("unexpected row " + entry.getKey() + " " + i);
      }
      
      if (entry.getKey().getTimestamp() != ts) {
        throw new Exception("unexpected ts " + entry.getKey() + " " + ts);
      }
      
      if (Integer.parseInt(entry.getValue().toString()) != i) {
        throw new Exception("unexpected value " + entry + " " + i);
      }
    }
    
    if (iter.hasNext()) {
      throw new Exception("found more than expected " + iter.next());
    }
    
  }
  
  private void insertData(long ts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException {
    BatchWriter bw = getConnector().createBatchWriter("foo", 10000000, 60000l, 3);
    
    for (int i = 0; i < 10000; i++) {
      String row = String.format("%09d", i);
      
      Mutation m = new Mutation(new Text(row));
      m.put(new Text("cf1"), new Text("cq1"), ts, new Value(("" + i).getBytes()));
      bw.addMutation(m);
    }
    
    bw.close();
  }
  
}
