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

import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * The program reads an accumulo table written by {@link Index} and writes out to another table. It writes out a mapping of documents to terms. The document to
 * term mapping is used by {@link ContinuousQuery}.
 * 
 * See docs/examples/README.shard for instructions.
 */

public class Reverse {
  public static void main(String[] args) throws Exception {
    
    if (args.length != 6) {
      System.err.println("Usage : " + Reverse.class.getName() + " <instance> <zoo keepers> <shard table> <doc2word table> <user> <pass>");
      System.exit(-1);
    }
    
    String instance = args[0];
    String zooKeepers = args[1];
    String inTable = args[2];
    String outTable = args[3];
    String user = args[4];
    String pass = args[5];
    
    ZooKeeperInstance zki = new ZooKeeperInstance(instance, zooKeepers);
    Connector conn = zki.getConnector(user, pass.getBytes());
    
    Scanner scanner = conn.createScanner(inTable, Constants.NO_AUTHS);
    BatchWriter bw = conn.createBatchWriter(outTable, 50000000, 600000l, 4);
    
    for (Entry<Key,Value> entry : scanner) {
      Key key = entry.getKey();
      Mutation m = new Mutation(key.getColumnQualifier());
      m.put(key.getColumnFamily(), new Text(), new Value(new byte[0]));
      bw.addMutation(m);
    }
    
    bw.close();
    
  }
}
