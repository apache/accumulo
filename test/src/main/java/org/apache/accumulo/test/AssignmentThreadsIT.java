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

import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

// ACCUMULO-1177
@Category(PerformanceTest.class)
public class AssignmentThreadsIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "1");
  }

  // [0-9a-f]
  private final static byte[] HEXCHARS = {0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66};
  private final static Random random = new Random();

  public static byte[] randomHex(int n) {
    byte[] binary = new byte[n];
    byte[] hex = new byte[n * 2];
    random.nextBytes(binary);
    int count = 0;
    for (byte x : binary) {
      hex[count++] = HEXCHARS[(x >> 4) & 0xf];
      hex[count++] = HEXCHARS[x & 0xf];
    }
    return hex;
  }

  @Test(timeout = 5 * 60 * 1000)
  public void testConcurrentAssignmentPerformance() throws Exception {
    // make a table with a lot of splits
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    log.info("Creating table");
    c.tableOperations().create(tableName);
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 1000; i++) {
      splits.add(new Text(randomHex(8)));
    }
    log.info("Adding splits");
    c.tableOperations().addSplits(tableName, splits);
    log.info("Taking table offline");
    c.tableOperations().offline(tableName, true);
    // time how long it takes to load
    log.info("Bringing the table online");
    long now = System.currentTimeMillis();
    c.tableOperations().online(tableName, true);
    long diff = System.currentTimeMillis() - now;
    log.info("Loaded " + splits.size() + " tablets in " + diff + " ms");
    c.instanceOperations().setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT.getKey(), "20");
    now = System.currentTimeMillis();
    log.info("Taking table offline, again");
    c.tableOperations().offline(tableName, true);
    // wait >10 seconds for thread pool to update
    sleepUninterruptibly(Math.max(0, now + 11 * 1000 - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
    now = System.currentTimeMillis();
    log.info("Bringing table back online");
    c.tableOperations().online(tableName, true);
    long diff2 = System.currentTimeMillis() - now;
    log.debug("Loaded " + splits.size() + " tablets in " + diff2 + " ms");
    assertTrue(diff2 < diff);
  }

}
