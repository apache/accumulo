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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;

/**
 * 
 */
public class TimeoutTest extends FunctionalTest {
  
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
    testBatchScannerTimeout();
    testBatchWriterTimeout();
  }

  public void testBatchWriterTimeout() throws Exception {
    Connector conn = getConnector();
    
    conn.tableOperations().create("foo1");
    
    conn.tableOperations().addConstraint("foo1", SlowConstraint.class.getName());
    
    // give constraint time to propogate through zookeeper
    UtilWaitThread.sleep(250);
    
    BatchWriter bw = conn.createBatchWriter("foo1", new BatchWriterConfig().setTimeout(3, TimeUnit.SECONDS));
    
    Mutation mut = new Mutation("r1");
    mut.put("cf1", "cq1", "v1");
    
    bw.addMutation(mut);
    try {
      bw.close();
      throw new Exception("batch writer did not timeout");
    } catch (MutationsRejectedException mre) {
      if (!(mre.getCause() instanceof TimedOutException)) {
        throw mre;
      }
    }
  }
  
  public void testBatchScannerTimeout() throws Exception {
    getConnector().tableOperations().create("timeout");
    
    BatchWriter bw = getConnector().createBatchWriter("timeout", new BatchWriterConfig());
    
    Mutation m = new Mutation("r1");
    m.put("cf1", "cq1", "v1");
    m.put("cf1", "cq2", "v2");
    m.put("cf1", "cq3", "v3");
    m.put("cf1", "cq4", "v4");
    
    bw.addMutation(m);
    
    bw.close();
    
    BatchScanner bs = getConnector().createBatchScanner("timeout", Constants.NO_AUTHS, 2);
    bs.setTimeout(1, TimeUnit.SECONDS);
    bs.setRanges(Collections.singletonList(new Range()));
    
    // should not timeout
    for (Entry<Key,Value> entry : bs) {
      entry.getKey();
    }
    
    IteratorSetting iterSetting = new IteratorSetting(100, SlowIterator.class);
    iterSetting.addOption("sleepTime", 2000 + "");
    getConnector().tableOperations().attachIterator("timeout", iterSetting);
    UtilWaitThread.sleep(250);

    try {
      for (Entry<Key,Value> entry : bs) {
        entry.getKey();
      }
      throw new Exception("batch scanner did not time out");
    } catch (TimedOutException toe) {
      // toe.printStackTrace();
    }

    bs.close();
  }
  
  @Override
  public void cleanup() throws Exception {
    
  }
  
}
