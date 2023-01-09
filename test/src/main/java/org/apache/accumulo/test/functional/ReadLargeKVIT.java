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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.bouncycastle.util.Arrays;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

public class ReadLargeKVIT extends SharedMiniClusterBase {

  
  private static class ReadLargeKVITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumTservers(1);
      cfg.setMemory(ServerType.TABLET_SERVER, 256, MemoryUnit.MEGABYTE);
      Map<String,String> sysProps = new HashMap<>();
      // Enable RFileMemoryProtection for any Value
      // over 10MB in size.
      sysProps.put("EnableRFileMemoryProtection", "true");
      sysProps.put("RFileMemoryProtectionSizeThreshold", Integer.toString(100 * 1024 * 1024));
      cfg.setSystemProperties(sysProps);
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    startMiniClusterWithConfig(new ReadLargeKVITConfiguration());
  }

  @AfterAll
  public static void stop() throws Exception {
    stopMiniCluster();
  }

  @Test
  public void test() throws Exception {

    final int NUM_ROWS = 10;
    byte[] EMPTY = new byte[0];
    byte[] VALUE = new byte[11 * 1024 * 1024];
    Arrays.fill(VALUE, (byte) '1');

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      String table = getUniqueNames(1)[0];
      TableOperations to = client.tableOperations();
      to.create(table);

      // Insert several K/V pairs with 11MB values
      BatchWriterConfig bwc = new BatchWriterConfig();
      bwc.setMaxMemory(24 * 1024 * 1024);
      try (BatchWriter writer = client.createBatchWriter(table, bwc)) {
        for (int i = 0; i < NUM_ROWS; i++) {
          Mutation m = new Mutation("test" + i);
          m.put(EMPTY, EMPTY, VALUE);
          writer.addMutation(m);
        }
      }

      // Validate NUM_ROWS in table
      long start = System.currentTimeMillis();
      try (Scanner scanner = client.createScanner(table)) {
        assertEquals(10, Iterables.size(scanner));
      }
      long firstRunTime = System.currentTimeMillis() - start;

      final AtomicReference<Throwable> ERROR = new AtomicReference<>();

      Thread t = new Thread(() -> {
        try (AccumuloClient client2 = Accumulo.newClient().from(getClientProps()).build()) {
          try (Scanner scanner2 = client2.createScanner(table)) {
            IteratorSetting is = new IteratorSetting(11, SlowMemoryConsumingIterator.class,
                Map.of("sleepTime", "30000"));
            scanner2.addScanIterator(is);
          } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
            e.printStackTrace();
            ERROR.set(e);
          }
        }
      });
      t.start();
      Thread.sleep(5000);

      long start2 = System.currentTimeMillis();
      try (Scanner scanner3 = client.createScanner(table)) {
        // SlowMemoryConsumingIterator is set to sleep for 30s
        // and consume all free memory except for 5MB. In theory,
        // getting the first K/V pair should take close to 30s.
        scanner3.setBatchSize(1);
        scanner3.setReadaheadThreshold(1);
        scanner3.setBatchTimeout(1, TimeUnit.MINUTES);
        Iterator<Entry<Key,Value>> iter = scanner3.iterator();
        assertTrue(iter.hasNext());
        Entry<Key,Value> e = iter.next();
        assertEquals(11 * 1024 * 1024, e.getValue().get().length);
      }
      long secondRunTime = System.currentTimeMillis() - start2;

      assertTrue(secondRunTime > firstRunTime);
      assertTrue(secondRunTime > 250000);
      t.join();
      assertNull(ERROR.get());

    }
  }

}
