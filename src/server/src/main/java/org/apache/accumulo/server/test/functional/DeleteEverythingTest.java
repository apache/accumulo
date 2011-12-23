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
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

public class DeleteEverythingTest extends FunctionalTest {
  
  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    Map<String,String> props = new HashMap<String,String>();
    props.put(Property.TSERV_MAJC_DELAY.getKey(), "1s");
    return props;
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.singletonList(new TableSetup("de"));
  }
  
  @Override
  public void run() throws Exception {
    BatchWriter bw = getConnector().createBatchWriter("de", 1000000, 60000l, 1);
    Mutation m = new Mutation(new Text("foo"));
    m.put(new Text("bar"), new Text("1910"), new Value("5".getBytes()));
    bw.addMutation(m);
    bw.flush();
    
    getConnector().tableOperations().flush("de", null, null, true);
    
    checkRFiles("de", 1, 1, 1, 1);
    
    m = new Mutation(new Text("foo"));
    m.putDelete(new Text("bar"), new Text("1910"));
    bw.addMutation(m);
    bw.flush();
    
    Scanner scanner = getConnector().createScanner("de", Constants.NO_AUTHS);
    scanner.setRange(new Range());
    
    int count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    if (count != 0)
      throw new Exception("count == " + count);
    
    getConnector().tableOperations().flush("de", null, null, true);
    
    getConnector().tableOperations().setProperty("de", Property.TABLE_MAJC_RATIO.getKey(), "1.0");
    UtilWaitThread.sleep(4000);
    
    checkRFiles("de", 1, 1, 0, 0);
    
    bw.close();
    
    count = 0;
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    
    if (count != 0)
      throw new Exception("count == " + count);
  }
}
