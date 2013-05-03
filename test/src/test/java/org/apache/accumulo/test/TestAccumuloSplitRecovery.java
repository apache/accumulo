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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;

import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.test.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAccumuloSplitRecovery {
  
  private static final String TABLE = "simple";
  public static TemporaryFolder folder = new TemporaryFolder();
  private MiniAccumuloCluster accumulo;
  private String secret = "secret";

  @Before
  public void setUp() throws Exception {
    folder.create();
    accumulo = new MiniAccumuloCluster(folder.getRoot(), secret);
    accumulo.start();
  }
  
  @After
  public void tearDown() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
  private Mutation m(String row) {
    Mutation result = new Mutation(row);
    result.put("cf", "cq", new Value("value".getBytes()));
    return result;
  }
  
  boolean isOffline(String tablename, Connector connector) throws TableNotFoundException {
    String tableId = connector.tableOperations().tableIdMap().get(tablename);
    Scanner scanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
    scanner.setRange(new Range(new Text(tableId + ";"), new Text(tableId + "<")));
    scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
    for (@SuppressWarnings("unused") Entry<Key,Value> entry : scanner) {
        return false;
    }
    return true;
  }
  
  @Test(timeout = 60000)
  public void test() throws Exception {
    
    for (int tn = 0; tn < 2; tn++) {
    
      ZooKeeperInstance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
      Connector connector = instance.getConnector("root", new PasswordToken(secret));
      // create a table and put some data in it
      connector.tableOperations().create(TABLE);
      BatchWriter bw = connector.createBatchWriter(TABLE, new BatchWriterConfig());
      bw.addMutation(m("a"));
      bw.addMutation(m("b"));
      bw.addMutation(m("c"));
      bw.close();
      // take the table offline
      connector.tableOperations().offline(TABLE);
      while (!isOffline(TABLE, connector))
        UtilWaitThread.sleep(200);
      
      // poke a partial split into the !METADATA table
      connector.securityOperations().grantTablePermission("root", Constants.METADATA_TABLE_NAME, TablePermission.WRITE);
      String tableId = connector.tableOperations().tableIdMap().get(TABLE);
      
      KeyExtent extent = new KeyExtent(new Text(tableId), null, new Text("b"));
      Mutation m = extent.getPrevRowUpdateMutation();
      
      Constants.METADATA_SPLIT_RATIO_COLUMN.put(m, new Value(Double.toString(0.5).getBytes()));
      Constants.METADATA_OLD_PREV_ROW_COLUMN.put(m, KeyExtent.encodePrevEndRow(null));
      bw = connector.createBatchWriter(Constants.METADATA_TABLE_NAME, new BatchWriterConfig());
      bw.addMutation(m);
      
      if (tn == 1) {
        
        bw.flush();
        
        Scanner scanner = connector.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
        scanner.setRange(extent.toMetadataRange());
        scanner.fetchColumnFamily(Constants.METADATA_DATAFILE_COLUMN_FAMILY);
        
        KeyExtent extent2 = new KeyExtent(new Text(tableId), new Text("b"), null);
        m = extent2.getPrevRowUpdateMutation();
        Constants.METADATA_DIRECTORY_COLUMN.put(m, new Value("/t2".getBytes()));
        Constants.METADATA_TIME_COLUMN.put(m, new Value("M0".getBytes()));
        
        for (Entry<Key,Value> entry : scanner) {
          m.put(Constants.METADATA_DATAFILE_COLUMN_FAMILY, entry.getKey().getColumnQualifier(), entry.getValue());
        }
        
        bw.addMutation(m);
      }
      
      bw.close();
      // bring the table online
      connector.tableOperations().online(TABLE);
      
      // verify the tablets went online
      Scanner scanner = connector.createScanner(TABLE, Constants.NO_AUTHS);
      int i = 0;
      String expected[] = {"a", "b", "c"};
      for (Entry<Key,Value> entry : scanner) {
        assertEquals(expected[i], entry.getKey().getRow().toString());
        i++;
      }
      assertEquals(3, i);

      connector.tableOperations().delete(TABLE);
    
    }
  }
  
}
