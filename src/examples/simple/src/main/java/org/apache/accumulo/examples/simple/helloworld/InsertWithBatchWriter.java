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
package org.apache.accumulo.examples.simple.helloworld;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Inserts 10K rows (50K entries) into accumulo with each row having 5 entries.
 */
public class InsertWithBatchWriter {
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException,
      TableNotFoundException {
    if (args.length != 5) {
      System.out
          .println("Usage: accumulo examples-simplejar accumulo.examples.helloworld.InsertWithBatchWriter <instance name> <zoo keepers> <username> <password> <tableName>");
      System.exit(1);
    }
    
    String instanceName = args[0];
    String zooKeepers = args[1];
    String user = args[2];
    byte[] pass = args[3].getBytes();
    String tableName = args[4];
    
    ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    Connector connector = instance.getConnector(user, pass);
    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(200000l, 300, 4);
    
    BatchWriter bw = null;
    
    if (!connector.tableOperations().exists(tableName))
      connector.tableOperations().create(tableName);
    bw = mtbw.getBatchWriter(tableName);
    
    Text colf = new Text("colfam");
    System.out.println("writing ...");
    for (int i = 0; i < 10000; i++) {
      Mutation m = new Mutation(new Text(String.format("row_%d", i)));
      for (int j = 0; j < 5; j++) {
        m.put(colf, new Text(String.format("colqual_%d", j)), new Value((String.format("value_%d_%d", i, j)).getBytes()));
      }
      bw.addMutation(m);
      if (i % 100 == 0)
        System.out.println(i);
    }
    
    mtbw.close();
  }
  
}
