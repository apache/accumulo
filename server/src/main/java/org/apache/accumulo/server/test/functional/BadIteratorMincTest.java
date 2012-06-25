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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

public class BadIteratorMincTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    String pre = Property.TABLE_ITERATOR_PREFIX.getKey();
    TableSetup ts = new TableSetup("foo", parseConfig(pre + "minc.badi=30," + BadIterator.class.getName()));
    
    return Collections.singletonList(ts);
  }
  
  @Override
  public void run() throws Exception {
    
    BatchWriter bw = getConnector().createBatchWriter("foo", 1000000, 60000l, 2);
    
    Mutation m = new Mutation(new Text("r1"));
    m.put(new Text("acf"), new Text("foo"), new Value("1".getBytes()));
    
    bw.addMutation(m);
    
    bw.close();
    
    getConnector().tableOperations().flush("foo", null, null, false);
    UtilWaitThread.sleep(1000);
    
    // minc should fail, so there should be no files
    checkRFiles("foo", 1, 1, 0, 0);
    
    // try to scan table
    Scanner scanner = getConnector().createScanner("foo", Constants.NO_AUTHS);
    
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    if (count != 1)
      throw new Exception("Did not see expected # entries " + count);
    
    // remove the bad iterator
    getConnector().tableOperations().removeProperty("foo", Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.badi");
    
    UtilWaitThread.sleep(5000);
    
    // minc should complete
    checkRFiles("foo", 1, 1, 1, 1);
    
    count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    if (count != 1)
      throw new Exception("Did not see expected # entries " + count);
    
    // now try putting bad iterator back and deleting the table
    getConnector().tableOperations().setProperty("foo", Property.TABLE_ITERATOR_PREFIX.getKey() + "minc.badi", "30," + BadIterator.class.getName());
    bw = getConnector().createBatchWriter("foo", 1000000, 60000l, 2);
    m = new Mutation(new Text("r2"));
    m.put(new Text("acf"), new Text("foo"), new Value("1".getBytes()));
    bw.addMutation(m);
    bw.close();
    
    // make sure property is given time to propagate
    UtilWaitThread.sleep(1000);
    
    getConnector().tableOperations().flush("foo", null, null, false);
    
    // make sure the flush has time to start
    UtilWaitThread.sleep(1000);
    
    // this should not hang
    getConnector().tableOperations().delete("foo");

  }
  
}
