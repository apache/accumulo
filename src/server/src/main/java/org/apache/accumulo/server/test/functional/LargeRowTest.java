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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
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
import org.apache.accumulo.server.test.TestIngest;
import org.apache.hadoop.io.Text;

public class LargeRowTest extends FunctionalTest {
  
  private static final int SEED = 42;
  private static final String REG_TABLE_NAME = "lr";
  private static final String PRE_SPLIT_TABLE_NAME = "lrps";
  private static final int NUM_ROWS = 100;
  private static final int ROW_SIZE = 1 << 17;
  private static final int SPLIT_THRESH = ROW_SIZE * NUM_ROWS / 5;
  private static final int NUM_PRE_SPLITS = 9;
  
  @Override
  public void cleanup() {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    return parseConfig(Property.TSERV_MAJC_DELAY + "=1");
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    
    Random r = new Random();
    byte rowData[] = new byte[ROW_SIZE];
    r.setSeed(SEED + 1);
    
    TreeSet<Text> splitPoints = new TreeSet<Text>();
    
    for (int i = 0; i < NUM_PRE_SPLITS; i++) {
      r.nextBytes(rowData);
      TestIngest.toPrintableChars(rowData);
      splitPoints.add(new Text(rowData));
    }
    
    ArrayList<TableSetup> tables = new ArrayList<TableSetup>();
    
    tables.add(new TableSetup(REG_TABLE_NAME));
    tables.add(new TableSetup(PRE_SPLIT_TABLE_NAME, splitPoints));
    
    return tables;
    // return Collections.singletonList(new TableSetup(TABLE_NAME));
  }
  
  @Override
  public void run() throws Exception {
    
    // Logger logger = Logger.getLogger(Constants.CORE_PACKAGE_NAME);
    // logger.setLevel(Level.TRACE);
    
    test1();
    test2();
  }
  
  private void test1() throws Exception {
    
    basicTest(REG_TABLE_NAME, 0);
    
    getConnector().tableOperations().setProperty(REG_TABLE_NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "" + SPLIT_THRESH);
    
    UtilWaitThread.sleep(5000);
    
    checkSplits(REG_TABLE_NAME, 1, 9);
    
    verify(REG_TABLE_NAME);
  }
  
  private void test2() throws Exception {
    basicTest(PRE_SPLIT_TABLE_NAME, NUM_PRE_SPLITS);
  }
  
  private void basicTest(String table, int expectedSplits) throws Exception {
    BatchWriter bw = getConnector().createBatchWriter(table, 10000000l, 60l, 3);
    
    Random r = new Random();
    byte rowData[] = new byte[ROW_SIZE];
    
    r.setSeed(SEED);
    
    for (int i = 0; i < NUM_ROWS; i++) {
      
      r.nextBytes(rowData);
      TestIngest.toPrintableChars(rowData);
      
      Mutation mut = new Mutation(new Text(rowData));
      mut.put(new Text(""), new Text(""), new Value(("" + i).getBytes()));
      bw.addMutation(mut);
    }
    
    bw.close();
    
    checkSplits(table, expectedSplits, expectedSplits);
    
    verify(table);
    
    checkSplits(table, expectedSplits, expectedSplits);
    
    getConnector().tableOperations().flush(table, null, null, false);
    
    // verify while table flush is running
    verify(table);
    
    // give flush time to complete
    UtilWaitThread.sleep(4000);
    
    checkSplits(table, expectedSplits, expectedSplits);
    
    verify(table);
    
    checkSplits(table, expectedSplits, expectedSplits);
  }
  
  private void verify(String table) throws Exception {
    Random r = new Random();
    byte rowData[] = new byte[ROW_SIZE];
    
    r.setSeed(SEED);
    
    Scanner scanner = getConnector().createScanner(table, Constants.NO_AUTHS);
    
    for (int i = 0; i < NUM_ROWS; i++) {
      
      r.nextBytes(rowData);
      TestIngest.toPrintableChars(rowData);
      
      scanner.setRange(new Range(new Text(rowData)));
      
      int count = 0;
      
      for (Entry<Key,Value> entry : scanner) {
        if (!entry.getKey().getRow().equals(new Text(rowData))) {
          throw new Exception("verification failed, unexpected row i =" + i);
        }
        if (!entry.getValue().equals(Integer.toString(i).getBytes())) {
          throw new Exception("verification failed, unexpected value i =" + i + " value = " + entry.getValue());
        }
        count++;
      }
      
      if (count != 1) {
        throw new Exception("verification failed, unexpected count i =" + i + " count=" + count);
      }
      
    }
    
  }
  
}
