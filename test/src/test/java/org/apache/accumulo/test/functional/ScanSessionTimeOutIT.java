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

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ScanSessionTimeOutIT extends MacTest {
  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setSiteConfig(Collections.singletonMap(Property.TSERV_SESSION_MAXIDLE.getKey(), "3"));
  }

  @Test(timeout=30*1000)
  public void run() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("abc");
    
    BatchWriter bw = c.createBatchWriter("abc", new BatchWriterConfig());
    
    for (int i = 0; i < 100000; i++) {
      Mutation m = new Mutation(new Text(String.format("%08d", i)));
      for (int j = 0; j < 3; j++)
        m.put(new Text("cf1"), new Text("cq" + j), new Value(("" + i + "_" + j).getBytes()));
      
      bw.addMutation(m);
    }
    
    bw.close();
    
    Scanner scanner = c.createScanner("abc", new Authorizations());
    scanner.setBatchSize(1000);
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    
    verify(iter, 0, 200);
    
    // sleep three times the session timeout
    UtilWaitThread.sleep(9000);
    
    verify(iter, 200, 100000);
    
  }
  
  private void verify(Iterator<Entry<Key,Value>> iter, int start, int stop) throws Exception {
    for (int i = start; i < stop; i++) {
      
      Text er = new Text(String.format("%08d", i));
      
      for (int j = 0; j < 3; j++) {
        Entry<Key,Value> entry = iter.next();
        
        if (!entry.getKey().getRow().equals(er)) {
          throw new Exception("row " + entry.getKey().getRow() + " != " + er);
        }
        
        if (!entry.getKey().getColumnFamily().equals(new Text("cf1"))) {
          throw new Exception("cf " + entry.getKey().getColumnFamily() + " != cf1");
        }
        
        if (!entry.getKey().getColumnQualifier().equals(new Text("cq" + j))) {
          throw new Exception("cq " + entry.getKey().getColumnQualifier() + " != cq" + j);
        }
        
        if (!entry.getValue().toString().equals("" + i + "_" + j)) {
          throw new Exception("value " + entry.getValue() + " != " + i + "_" + j);
        }
        
      }
    }
    
  }
  
}
