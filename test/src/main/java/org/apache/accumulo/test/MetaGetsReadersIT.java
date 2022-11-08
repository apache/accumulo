/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;

public class MetaGetsReadersIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.TSERV_SCAN_MAX_OPENFILES, "2");
    cfg.setProperty(Property.TABLE_BLOCKCACHE_ENABLED, "false");
  }

  private static Thread slowScan(final AccumuloClient c, final String tableName,
      final AtomicBoolean stop) {
    return new Thread(() -> {
      try {
        while (!stop.get()) {
          try (Scanner s = c.createScanner(tableName, Authorizations.EMPTY)) {
            IteratorSetting is = new IteratorSetting(50, SlowIterator.class);
            SlowIterator.setSleepTime(is, 10);
            s.addScanIterator(is);
            Iterator<Entry<Key,Value>> iterator = s.iterator();
            while (iterator.hasNext() && !stop.get()) {
              iterator.next();
            }
          }
        }
      } catch (Exception ex) {
        log.trace("{}", ex.getMessage(), ex);
        stop.set(true);
      }
    });
  }

  public void test() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.tableOperations().create(tableName);
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < 50000; i++) {
          byte[] row = new byte[100];
          random.nextBytes(row);
          Mutation m = new Mutation(row);
          m.put("", "", "");
          bw.addMutation(m);
        }
      }
      c.tableOperations().flush(tableName, null, null, true);
      final AtomicBoolean stop = new AtomicBoolean(false);
      Thread t1 = slowScan(c, tableName, stop);
      t1.start();
      Thread t2 = slowScan(c, tableName, stop);
      t2.start();
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      long now = System.currentTimeMillis();

      try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        s.forEach((k, v) -> {});
      }

      long delay = System.currentTimeMillis() - now;
      System.out.println("Delay = " + delay);
      assertTrue(delay < 1000, "metadata table scan was slow");
      assertFalse(stop.get());
      stop.set(true);
      t1.interrupt();
      t2.interrupt();
      t1.join();
      t2.join();
    }
  }

}
