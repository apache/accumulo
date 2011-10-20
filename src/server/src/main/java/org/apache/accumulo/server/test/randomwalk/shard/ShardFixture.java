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
package org.apache.accumulo.server.test.randomwalk.shard;

import java.net.InetAddress;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class ShardFixture extends Fixture {
  
  static SortedSet<Text> genSplits(long max, int numTablets, String format) {
    
    int numSplits = numTablets - 1;
    long distance = (max / numTablets);
    long split = distance;
    
    TreeSet<Text> splits = new TreeSet<Text>();
    
    for (int i = 0; i < numSplits; i++) {
      splits.add(new Text(String.format(format, split)));
      split += distance;
    }
    
    return splits;
  }
  
  static void createIndexTable(Logger log, State state, String suffix, Random rand) throws Exception {
    Connector conn = state.getConnector();
    String name = (String) state.get("indexTableName") + suffix;
    int numPartitions = (Integer) state.get("numPartitions");
    boolean enableCache = (Boolean) state.get("cacheIndex");
    conn.tableOperations().create(name);
    conn.tableOperations().addSplits(name, genSplits(numPartitions, rand.nextInt(numPartitions) + 1, "%06x"));
    
    if (enableCache) {
      conn.tableOperations().setProperty(name, Property.TABLE_INDEXCACHE_ENABLED.getKey(), "true");
      conn.tableOperations().setProperty(name, Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
      
      log.info("Enabled caching for table " + name);
    }
  }
  
  @Override
  public void setUp(State state) throws Exception {
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    String pid = state.getPid();
    
    Random rand = new Random();
    
    int numPartitions = rand.nextInt(90) + 10;
    
    state.set("indexTableName", String.format("ST_index_%s_%s_%d", hostname, pid, System.currentTimeMillis()));
    state.set("docTableName", String.format("ST_docs_%s_%s_%d", hostname, pid, System.currentTimeMillis()));
    state.set("numPartitions", new Integer(numPartitions));
    state.set("cacheIndex", rand.nextDouble() < .5);
    state.set("rand", rand);
    state.set("nextDocID", new Long(0));
    
    Connector conn = state.getConnector();
    
    createIndexTable(this.log, state, "", rand);
    
    String docTableName = (String) state.get("docTableName");
    conn.tableOperations().create(docTableName);
    conn.tableOperations().addSplits(docTableName, genSplits(0xff, rand.nextInt(32) + 1, "%02x"));
    
    if (rand.nextDouble() < .5) {
      conn.tableOperations().setProperty((String) state.get("docTableName"), Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      log.info("Enabled bloom filters for table " + (String) state.get("docTableName"));
    }
  }
  
  @Override
  public void tearDown(State state) throws Exception {
    Connector conn = state.getConnector();
    
    conn.tableOperations().delete((String) state.get("indexTableName"));
    conn.tableOperations().delete((String) state.get("docTableName"));
    
    log.debug("Exiting shard test");
  }
  
}
