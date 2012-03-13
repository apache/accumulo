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
package org.apache.accumulo.examples.simple.shard;

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.hadoop.io.Text;

/**
 * This program queries a set of terms in the shard table (populated by {@link Index}) using the {@link IntersectingIterator}.
 * 
 * See docs/examples/README.shard for instructions.
 */

public class Query {
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    
    if (args.length < 6) {
      System.err.println("Usage : " + Query.class.getName() + " <instance> <zoo keepers> <table> <user> <pass> <term>{ <term>}");
      System.exit(-1);
    }
    
    String instance = args[0];
    String zooKeepers = args[1];
    String table = args[2];
    String user = args[3];
    String pass = args[4];
    
    ZooKeeperInstance zki = new ZooKeeperInstance(instance, zooKeepers);
    Connector conn = zki.getConnector(user, pass.getBytes());
    
    BatchScanner bs = conn.createBatchScanner(table, Constants.NO_AUTHS, 20);
    
    Text columns[] = new Text[args.length - 5];
    for (int i = 5; i < args.length; i++) {
      columns[i - 5] = new Text(args[i]);
    }
    IteratorSetting ii = new IteratorSetting(20, "ii", IntersectingIterator.class);
    IntersectingIterator.setColumnFamilies(ii, columns);
    bs.addScanIterator(ii);
    bs.setRanges(Collections.singleton(new Range()));
    for (Entry<Key,Value> entry : bs) {
      System.out.println("  " + entry.getKey().getColumnQualifier());
    }
    
  }
  
}
