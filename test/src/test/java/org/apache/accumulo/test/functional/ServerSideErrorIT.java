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
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ServerSideErrorIT extends MacTest {
  
  @Test(timeout=60*1000)
  public void run() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("tt");
    IteratorSetting is = new IteratorSetting(5, "Bad Aggregator", BadCombiner.class);
    Combiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("acf")));
    c.tableOperations().attachIterator("tt", is);
    
    BatchWriter bw = c.createBatchWriter("tt", new BatchWriterConfig());
    
    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("acf"), new Text("foo"), new Value("1".getBytes()));
    
    bw.addMutation(m);
    
    bw.close();
    
    // try to scan table
    Scanner scanner = c.createScanner("tt", Authorizations.EMPTY);
    
    boolean caught = false;
    try {
      for (Entry<Key,Value> entry : scanner) {
        entry.getKey();
      }
    } catch (Exception e) {
      caught = true;
    }
    
    if (!caught)
      throw new Exception("Scan did not fail");
    
    // try to batch scan the table
    BatchScanner bs = c.createBatchScanner("tt", Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));
    
    caught = false;
    try {
      for (Entry<Key,Value> entry : bs) {
        entry.getKey();
      }
    } catch (Exception e) {
      caught = true;
    } finally {
      bs.close();
    }
    
    if (!caught)
      throw new Exception("batch scan did not fail");
    
    // remove the bad agg so accumulo can shutdown
    TableOperations to = c.tableOperations();
    for (Entry<String,String> e : to.getProperties("tt")) {
      to.removeProperty("tt", e.getKey());
    }
    
    UtilWaitThread.sleep(500);
    
    // should be able to scan now
    scanner = c.createScanner("tt", Authorizations.EMPTY);
    for (Entry<Key,Value> entry : scanner) {
      entry.getKey();
    }
    
    // set a non existant iterator, should cause scan to fail on server side
    scanner.addScanIterator(new IteratorSetting(100, "bogus", "com.bogus.iterator"));
    
    caught = false;
    try {
      for (Entry<Key,Value> entry : scanner) {
        // should error
        entry.getKey();
      }
    } catch (Exception e) {
      caught = true;
    }
    
    if (!caught)
      throw new Exception("Scan did not fail");
  }
}
