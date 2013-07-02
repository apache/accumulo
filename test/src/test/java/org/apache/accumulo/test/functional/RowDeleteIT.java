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

import static org.apache.accumulo.test.functional.FunctionalTestUtils.checkRFiles;
import static org.apache.accumulo.test.functional.FunctionalTestUtils.nm;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class RowDeleteIT extends MacTest {

  
  @Override
  public void configure(MiniAccumuloConfig cfg) {
    cfg.setSiteConfig(Collections.singletonMap(Property.TSERV_MAJC_DELAY.getKey(), "50ms"));
  }

  @Test(timeout=30*1000)
  public void run() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create("rdel1");
    Map<String,Set<Text>> groups = new HashMap<String, Set<Text>>();
    groups.put("lg1", Collections.singleton(new Text("foo")));
    groups.put("dg", Collections.<Text>emptySet());
    c.tableOperations().setLocalityGroups("rdel1", groups);
    IteratorSetting setting = new IteratorSetting(30, RowDeletingIterator.class);
    c.tableOperations().attachIterator("rdel1", setting, EnumSet.of(IteratorScope.majc));
    c.tableOperations().setProperty("rdel1", Property.TABLE_MAJC_RATIO.getKey(), "100");
    
    BatchWriter bw = c.createBatchWriter("rdel1", new BatchWriterConfig());
    
    bw.addMutation(nm("r1", "foo", "cf1", "v1"));
    bw.addMutation(nm("r1", "bar", "cf1", "v2"));
    
    bw.flush();
    c.tableOperations().flush("rdel1", null, null, true);
    
    checkRFiles(c, "rdel1", 1, 1, 1, 1);
    
    int count = 0;
    Scanner scanner = c.createScanner("rdel1", Authorizations.EMPTY);
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    if (count != 2)
      throw new Exception("1 count=" + count);
    
    bw.addMutation(nm("r1", "", "", RowDeletingIterator.DELETE_ROW_VALUE));
    
    bw.flush();
    c.tableOperations().flush("rdel1", null, null, true);
    
    checkRFiles(c, "rdel1", 1, 1, 2, 2);
    
    count = 0;
    scanner = c.createScanner("rdel1", Authorizations.EMPTY);
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    if (count != 3)
      throw new Exception("2 count=" + count);
    
    c.tableOperations().compact("rdel1", null, null, false, true);
    
    checkRFiles(c, "rdel1", 1, 1, 0, 0);
    
    count = 0;
    scanner = c.createScanner("rdel1", Authorizations.EMPTY);
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : scanner) {
      count++;
    }
    if (count != 0)
      throw new Exception("3 count=" + count);
    
    bw.close();
    
  }
  
}
