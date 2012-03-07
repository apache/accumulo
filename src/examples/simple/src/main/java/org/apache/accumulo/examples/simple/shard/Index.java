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

import java.io.File;
import java.io.FileReader;
import java.util.HashSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * This program indexes a set of documents given on the command line into a shard table.
 * 
 * What it writes to the table is row = partition id, column family = term, column qualifier = document id.
 * 
 * See docs/examples/README.shard for instructions.
 */

public class Index {
  
  static Text genPartition(int partition) {
    return new Text(String.format("%08x", Math.abs(partition)));
  }
  
  public static void index(int numPartitions, Text docId, String doc, String splitRegex, BatchWriter bw) throws Exception {
    
    String[] tokens = doc.split(splitRegex);
    
    Text partition = genPartition(doc.hashCode() % numPartitions);
    
    Mutation m = new Mutation(partition);
    
    HashSet<String> tokensSeen = new HashSet<String>();
    
    for (String token : tokens) {
      token = token.toLowerCase();
      
      if (!tokensSeen.contains(token)) {
        tokensSeen.add(token);
        m.put(new Text(token), docId, new Value(new byte[0]));
      }
    }
    
    if (m.size() > 0)
      bw.addMutation(m);
  }
  
  private static void index(int numPartitions, File src, String splitRegex, BatchWriter bw) throws Exception {
    
    if (src.isDirectory()) {
      for (File child : src.listFiles()) {
        index(numPartitions, child, splitRegex, bw);
      }
    } else {
      FileReader fr = new FileReader(src);
      
      StringBuilder sb = new StringBuilder();
      
      char data[] = new char[4096];
      int len;
      while ((len = fr.read(data)) != -1) {
        sb.append(data, 0, len);
      }
      
      fr.close();
      
      index(numPartitions, new Text(src.getAbsolutePath()), sb.toString(), splitRegex, bw);
    }
    
  }
  
  private static BatchWriter setupBatchWriter(String instance, String zooKeepers, String table, String user, String pass) throws Exception {
    ZooKeeperInstance zinstance = new ZooKeeperInstance(instance, zooKeepers);
    Connector conn = zinstance.getConnector(user, pass.getBytes());
    return conn.createBatchWriter(table, 50000000, 300000l, 4);
  }
  
  public static void main(String[] args) throws Exception {
    
    if (args.length < 7) {
      System.err.println("Usage : " + Index.class.getName() + " <instance> <zoo keepers> <table> <user> <pass> <num partitions> <file>{ <file>}");
      System.exit(-1);
    }
    
    String instance = args[0];
    String zooKeepers = args[1];
    String table = args[2];
    String user = args[3];
    String pass = args[4];
    
    int numPartitions = Integer.parseInt(args[5]);
    
    String splitRegex = "\\W+";
    
    BatchWriter bw = setupBatchWriter(instance, zooKeepers, table, user, pass);
    
    for (int i = 6; i < args.length; i++) {
      index(numPartitions, new File(args[i]), splitRegex, bw);
    }
    
    bw.close();
    
  }
  
}
