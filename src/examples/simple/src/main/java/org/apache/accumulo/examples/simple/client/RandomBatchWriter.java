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

import java.util.HashSet;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

/**
 * Simple example for writing random data to Accumulo. See docs/examples/README.batch for instructions.
 * 
 * The rows of the entries will be randomly generated numbers between a specified min and max (prefixed by "row_"). The column families will be "foo" and column
 * qualifiers will be "1". The values will be random byte arrays of a specified size.
 */
public class RandomBatchWriter {
  
  /**
   * Creates a random byte array of specified size using the specified seed.
   * 
   * @param rowid
   *          the seed to use for the random number generator
   * @param dataSize
   *          the size of the array
   * @return a random byte array
   */
  public static byte[] createValue(long rowid, int dataSize) {
    Random r = new Random(rowid);
    byte value[] = new byte[dataSize];
    
    r.nextBytes(value);
    
    // transform to printable chars
    for (int j = 0; j < value.length; j++) {
      value[j] = (byte) (((0xff & value[j]) % 92) + ' ');
    }
    
    return value;
  }
  
  /**
   * Creates a mutation on a specified row with column family "foo", column qualifier "1", specified visibility, and a random value of specified size.
   * 
   * @param rowid
   *          the row of the mutation
   * @param dataSize
   *          the size of the random value
   * @param visibility
   *          the visibility of the entry to insert
   * @return a mutation
   */
  public static Mutation createMutation(long rowid, int dataSize, ColumnVisibility visibility) {
    Text row = new Text(String.format("row_%010d", rowid));
    
    Mutation m = new Mutation(row);
    
    // create a random value that is a function of the
    // row id for verification purposes
    byte value[] = createValue(rowid, dataSize);
    
    m.put(new Text("foo"), new Text("1"), visibility, new Value(value));
    
    return m;
  }
  
  /**
   * Writes a specified number of entries to Accumulo using a {@link BatchWriter}.
   * 
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   * @throws TableNotFoundException
   */
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    
    String seed = null;
    
    int index = 0;
    String processedArgs[] = new String[13];
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-s")) {
        seed = args[++i];
      } else {
        processedArgs[index++] = args[i];
      }
    }
    
    if (index != 13) {
      System.out
          .println("Usage : RandomBatchWriter [-s <seed>] <instance name> <zoo keepers> <username> <password> <table> <num> <min> <max> <value size> <max memory> <max latency> <num threads> <visibility>");
      System.exit(1);
    }
    
    String instanceName = processedArgs[0];
    String zooKeepers = processedArgs[1];
    String user = processedArgs[2];
    byte[] pass = processedArgs[3].getBytes();
    String table = processedArgs[4];
    int num = Integer.parseInt(processedArgs[5]);
    long min = Long.parseLong(processedArgs[6]);
    long max = Long.parseLong(processedArgs[7]);
    int valueSize = Integer.parseInt(processedArgs[8]);
    long maxMemory = Long.parseLong(processedArgs[9]);
    long maxLatency = Long.parseLong(processedArgs[10]) == 0 ? Long.MAX_VALUE : Long.parseLong(processedArgs[10]);
    int numThreads = Integer.parseInt(processedArgs[11]);
    String visiblity = processedArgs[12];

    if ((max - min) < num) {
      System.err.println(String.format("You must specify a min and a max that allow for at least num possible values. For example, you requested %d rows, but a min of %d and a max of %d only allows for %d rows.", num, min, max, (max-min)));
      System.exit(1);
    }
    
    // Uncomment the following lines for detailed debugging info
    // Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    // logger.setLevel(Level.TRACE);
    
    Random r;
    if (seed == null)
      r = new Random();
    else {
      r = new Random(Long.parseLong(seed));
    }
    
    ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
    Connector connector = instance.getConnector(user, pass);
    BatchWriter bw = connector.createBatchWriter(table, maxMemory, maxLatency, numThreads);
    
    // reuse the ColumnVisibility object to improve performance
    ColumnVisibility cv = new ColumnVisibility(visiblity);
    
    // Generate num unique row ids in the given range
    HashSet<Long> rowids = new HashSet<Long>(num);
    while (rowids.size() < num) {
      rowids.add((Math.abs(r.nextLong()) % (max - min)) + min);
    }
    for (long rowid : rowids) {
      
      Mutation m = createMutation(rowid, valueSize, cv);
      
      bw.addMutation(m);
      
    }
    
    try {
      bw.close();
    } catch (MutationsRejectedException e) {
      if (e.getAuthorizationFailures().size() > 0) {
        HashSet<String> tables = new HashSet<String>();
        for (KeyExtent ke : e.getAuthorizationFailures()) {
          tables.add(ke.getTableId().toString());
        }
        System.err.println("ERROR : Not authorized to write to tables : " + tables);
      }
      
      if (e.getConstraintViolationSummaries().size() > 0) {
        System.err.println("ERROR : Constraint violations occurred : " + e.getConstraintViolationSummaries());
      }
      System.exit(1);
    }
  }
}
