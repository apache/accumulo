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
package org.apache.accumulo.examples.simple.client;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Simple example for writing random data in sequential order to Accumulo. See docs/examples/README.batch for instructions.
 */
public class SequentialBatchWriter {
  /**
   * Writes a specified number of entries to Accumulo using a {@link BatchWriter}. The rows of the entries will be sequential starting at a specified number.
   * The column families will be "foo" and column qualifiers will be "1". The values will be random byte arrays of a specified size.
   * 
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   * @throws TableNotFoundException
   * @throws MutationsRejectedException
   */
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, MutationsRejectedException {
    if (args.length != 12) {
      System.out
          .println("Usage : SequentialBatchWriter <instance name> <zoo keepers> <username> <password> <table> <start> <num> <value size> <max memory> <max latency> <num threads> <visibility>");
      return;
    }
    
    String instanceName = args[0];
    String zooKeepers = args[1];
    String user = args[2];
    byte[] pass = args[3].getBytes();
    String table = args[4];
    long start = Long.parseLong(args[5]);
    long num = Long.parseLong(args[6]);
    int valueSize = Integer.parseInt(args[7]);
    long maxMemory = Long.parseLong(args[8]);
    long maxLatency = Long.parseLong(args[9]) == 0 ? Long.MAX_VALUE : Long.parseLong(args[9]);
    int numThreads = Integer.parseInt(args[10]);
    String visibility = args[11];
    
    // Uncomment the following lines for detailed debugging info
    // Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    // logger.setLevel(Level.TRACE);
    
    ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    Connector connector = instance.getConnector(user, pass);
    BatchWriter bw = connector.createBatchWriter(table, maxMemory, maxLatency, numThreads);
    
    long end = start + num;
    
    // reuse ColumnVisibility object for better performance
    ColumnVisibility cv = new ColumnVisibility(visibility);
    
    for (long i = start; i < end; i++) {
      Mutation m = RandomBatchWriter.createMutation(i, valueSize, cv);
      bw.addMutation(m);
    }
    
    bw.close();
  }
}
