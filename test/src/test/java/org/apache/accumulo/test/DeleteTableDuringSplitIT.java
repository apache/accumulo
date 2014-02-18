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

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// ACCUMULO-2361
public class DeleteTableDuringSplitIT {
  public static TemporaryFolder folder = new TemporaryFolder();
  private MiniAccumuloCluster accumulo;
  private String secret = "secret";
  
  Connector getConnector() throws AccumuloException, AccumuloSecurityException {
    ZooKeeperInstance zki = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    return zki.getConnector("root", new PasswordToken(secret));
  }
  
  String[] getTableNames(int n) {
    String[] result = new String[n];
    for (int i = 0; i < n; i++) {
      result[i] = "test_" + i;
    }
    return result;
  }
  
  @Before
  public void setUp() throws Exception {
    folder.create();
    accumulo = new MiniAccumuloCluster(folder.getRoot(), secret);
    accumulo.start();
  }
  
  @After
  public void tearDown() throws Exception {
    accumulo.stop();
    //folder.delete();
  }
  

  @Test(timeout= 10 * 60 * 1000)
  public void test() throws Exception {
    String[] tableNames = getTableNames(100);
    // make a bunch of tables
    for (String tableName : tableNames) {
      getConnector().tableOperations().create(tableName);
    }
    final SortedSet<Text> splits = new TreeSet<Text>();
    for (byte i = 0; i < 100; i++) {
      splits.add(new Text(new byte[]{0, 0, i}));
    }

    List<Future<?>> results = new ArrayList<Future<?>>();
    List<Runnable> tasks = new ArrayList<Runnable>();
    SimpleThreadPool es = new SimpleThreadPool(tableNames.length, "concurrent-api-requests");
    for (String tableName : tableNames) {
      final String finalName = tableName;
      tasks.add(new Runnable() {
        @Override
        public void run() {
          try {
            getConnector().tableOperations().addSplits(finalName, splits);
          } catch (TableNotFoundException ex) {
          } catch (Exception ex) {
            throw new RuntimeException(finalName, ex);
          }
        }
      });
      tasks.add(new Runnable() {
        @Override
        public void run() {
          try {
            UtilWaitThread.sleep(500);
            getConnector().tableOperations().delete(finalName);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      });
    }
    for (Runnable r : tasks)
      results.add(es.submit(r));
    for (Future<?> f : results) {
      f.get();
    }
    for (String tableName : tableNames) {
      assertFalse(getConnector().tableOperations().exists(tableName));
    }
  }

}
