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
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class MetaGetsReadersIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.TSERV_SCAN_MAX_OPENFILES, "2");
    cfg.setProperty(Property.TABLE_BLOCKCACHE_ENABLED, "false");
  }

  private static Thread slowScan(final Connector c, final String tableName, final AtomicBoolean stop) {
    Thread thread = new Thread() {
      public void run() {
        try {
          while (stop.get() == false) {
            Scanner s = c.createScanner(tableName, Authorizations.EMPTY);
            IteratorSetting is = new IteratorSetting(50, SlowIterator.class);
            SlowIterator.setSleepTime(is, 10);
            s.addScanIterator(is);
            Iterator<Entry<Key,Value>> iterator = s.iterator();
            while (iterator.hasNext() && stop.get() == false) {
              iterator.next();
            }
          }
        } catch (Exception ex) {
          log.trace(ex, ex);
          stop.set(true);
        }
      }
    };
    return thread;
  }

  @Test(timeout = 2 * 60 * 1000)
  public void test() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    final Connector c = getConnector();
    c.tableOperations().create(tableName);
    Random random = new Random();
    BatchWriter bw = c.createBatchWriter(tableName, null);
    for (int i = 0; i < 50000; i++) {
      byte[] row = new byte[100];
      random.nextBytes(row);
      Mutation m = new Mutation(row);
      m.put("", "", "");
      bw.addMutation(m);
    }
    bw.close();
    c.tableOperations().flush(tableName, null, null, true);
    final AtomicBoolean stop = new AtomicBoolean(false);
    Thread t1 = slowScan(c, tableName, stop);
    t1.start();
    Thread t2 = slowScan(c, tableName, stop);
    t2.start();
    UtilWaitThread.sleep(500);
    long now = System.currentTimeMillis();
    Scanner m = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    for (@SuppressWarnings("unused")
    Entry<Key,Value> entry : m) {}
    long delay = System.currentTimeMillis() - now;
    System.out.println("Delay = " + delay);
    assertTrue("metadata table scan was slow", delay < 1000);
    assertFalse(stop.get());
    stop.set(true);
    t1.interrupt();
    t2.interrupt();
    t1.join();
    t2.join();
  }

}
