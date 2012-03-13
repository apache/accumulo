/**
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


// attempt to reproduce ACCUMULO-315
public class DeleteRowsSplitTest extends FunctionalTest {
  
  private static final Logger log = Logger.getLogger(DeleteRowsSplitTest.class);
  
  private static final String LETTERS = "abcdefghijklmnopqrstuvwxyz";
  static final SortedSet<Text> SPLITS = new TreeSet<Text>();
  static final List<String> ROWS = new ArrayList<String>();
  static {
    for (byte b : LETTERS.getBytes()) {
      SPLITS.add(new Text(new byte[] {b}));
      ROWS.add(new String(new byte[] {b}));
    }
  }

  @Override
  public void cleanup() throws Exception {}
  
  @Override
  public Map<String,String> getInitialConfig() {
    return Collections.emptyMap();
  }
  
  @Override
  public List<TableSetup> getTablesToCreate() {
    return Collections.emptyList();
  }
  
  static final String TABLE;
  static {
    Random random = new Random();
    TABLE = "table" + Long.toHexString(random.nextLong());
  }

  @Override
  public void run() throws Exception {
    // Delete ranges of rows, and verify the are removed
    // Do this while adding many splits
    
    // Eliminate whole tablets
    for (int test = 0; test < 50; test++) {
      // create a table
      log.info("Test " + test);
      getConnector().tableOperations().create(TABLE);

      // put some data in it
      fillTable(TABLE);

      // generate a random delete range
      final Text start = new Text();
      final Text end = new Text();
      generateRandomRange(start, end);

      // initiate the delete range
      final boolean fail[] = { false };
      Thread t = new Thread() {
        public void run() {
          try {
            // split the table
            final SortedSet<Text> afterEnd = SPLITS.tailSet(new Text(end.toString() + "\0"));
            getConnector().tableOperations().addSplits(TABLE, afterEnd);
          } catch (Exception ex) {
            log.error(ex, ex);
            synchronized (fail) {
              fail[0] = true;
            }
          }
        }
      };
      t.start();
      
      UtilWaitThread.sleep(test * 2);

      getConnector().tableOperations().deleteRows(TABLE, start, end);
      
      t.join();
      synchronized (fail) {
        assertTrue(!fail[0]);
      }

      // scan the table
      Scanner scanner = getConnector().createScanner(TABLE, Constants.NO_AUTHS);
      for (Entry<Key,Value> entry : scanner) {
        Text row = entry.getKey().getRow();
        assertTrue(row.compareTo(start) <= 0 || row.compareTo(end) > 0);
      }
      
      // delete the table
      getConnector().tableOperations().delete(TABLE);
    }
  }
  
  private void generateRandomRange(Text start, Text end) {
    List<String> bunch = new ArrayList<String>(ROWS);
    Collections.shuffle(bunch);
    if (bunch.get(0).compareTo((bunch.get(1))) < 0) {
      start.set(bunch.get(0));
      end.set(bunch.get(1));
    } else {
      start.set(bunch.get(1));
      end.set(bunch.get(0));
    }

  }
  
  private void fillTable(String table) throws Exception {
    BatchWriter bw = getConnector().createBatchWriter(TABLE, 100000l, 1000l, 2);
    for (String row : ROWS) {
      Mutation m = new Mutation(row);
      m.put("cf", "cq", "value");
      bw.addMutation(m);
    }
    bw.close();
  }

  private void assertTrue(boolean b) {
    if (!b)
      throw new RuntimeException("test failed, false value");
  }
  
}

