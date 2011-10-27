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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

public class ConcurrencyTest extends FunctionalTest {
  
  static class ScanTask extends Thread {
    
    int count = 0;
    Scanner scanner;
    
    ScanTask(Connector conn, long time) throws Exception {
      scanner = conn.createScanner("cct", Constants.NO_AUTHS);
      IteratorSetting slow = new IteratorSetting(30, "slow", SlowIterator.class);
      slow.addOption("sleepTime", "" + time);
      scanner.addScanIterator(slow);
    }
    
    @Override
    public void run() {
      for (@SuppressWarnings("unused")
      Entry<Key,Value> entry : scanner) {
        count++;
      }
      
    }
    
  }
  
  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    HashMap<String,String> opts = new HashMap<String,String>();
    opts.put("tserver.compaction.major.delay", "1");
    return opts;
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    String pre = Property.TABLE_ITERATOR_PREFIX.getKey();
    TableSetup ts = new TableSetup("cct", parseConfig(pre + "minc.slow=30," + SlowIterator.class.getName(), pre + "minc.slow.opt.sleepTime=50", pre
        + "majc.slow=30," + SlowIterator.class.getName(), pre + "majc.slow.opt.sleepTime=50", Property.TABLE_MAJC_RATIO.getKey() + "=1"));
    
    return Collections.singletonList(ts);
  }
  
  /*
   * Below is a diagram of the operations in this test over time.
   * 
   * Scan 0 |------------------------------| Scan 1 |----------| Minc 1 |-----| Scan 2 |----------| Scan 3 |---------------| Minc 2 |-----| Majc 1 |-----|
   */
  
  @Override
  public void run() throws Exception {
    BatchWriter bw = getConnector().createBatchWriter("cct", 10000000, 60000l, 1);
    for (int i = 0; i < 50; i++) {
      Mutation m = new Mutation(new Text(String.format("%06d", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value("foo".getBytes()));
      bw.addMutation(m);
    }
    
    bw.flush();
    
    ScanTask st0 = new ScanTask(getConnector(), 300);
    st0.start();
    
    ScanTask st1 = new ScanTask(getConnector(), 100);
    st1.start();
    
    UtilWaitThread.sleep(50);
    getConnector().tableOperations().flush("cct", null, null, true);
    
    for (int i = 0; i < 50; i++) {
      Mutation m = new Mutation(new Text(String.format("%06d", i)));
      m.put(new Text("cf1"), new Text("cq1"), new Value("foo".getBytes()));
      bw.addMutation(m);
    }
    
    bw.flush();
    
    ScanTask st2 = new ScanTask(getConnector(), 100);
    st2.start();
    
    st1.join();
    st2.join();
    if (st1.count != 50)
      throw new Exception("Thread 1 did not see 50, saw " + st1.count);
    
    if (st2.count != 50)
      throw new Exception("Thread 2 did not see 50, saw " + st2.count);
    
    ScanTask st3 = new ScanTask(getConnector(), 150);
    st3.start();
    
    UtilWaitThread.sleep(50);
    getConnector().tableOperations().flush("cct", null, null, false);
    
    st3.join();
    if (st3.count != 50)
      throw new Exception("Thread 3 did not see 50, saw " + st3.count);
    
    st0.join();
    if (st0.count != 50)
      throw new Exception("Thread 0 did not see 50, saw " + st0.count);
    
    bw.close();
  }
}
