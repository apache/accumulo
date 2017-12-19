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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class MultiTableRecoveryIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testRecoveryOverMultipleTables() throws Exception {
    final int N = 3;
    final Connector c = getConnector();
    final String[] tables = getUniqueNames(N);
    final BatchWriter[] writers = new BatchWriter[N];
    final byte[][] values = new byte[N][];
    int i = 0;
    System.out.println("Creating tables");
    for (String tableName : tables) {
      c.tableOperations().create(tableName);
      values[i] = Integer.toString(i).getBytes();
      writers[i] = c.createBatchWriter(tableName, null);
      i++;
    }
    System.out.println("Creating agitator");
    final AtomicBoolean stop = new AtomicBoolean(false);
    final Thread agitator = agitator(stop);
    agitator.start();
    System.out.println("writing");
    final Random random = new Random();
    for (i = 0; i < 1_000_000; i++) {
      // make non-negative avoiding Math.abs, because that can still be negative
      long randomRow = random.nextLong() & Long.MAX_VALUE;
      assertTrue(randomRow >= 0);
      final int table = (int) (randomRow % N);
      final Mutation m = new Mutation(Long.toHexString(randomRow));
      m.put(new byte[0], new byte[0], values[table]);
      writers[table].addMutation(m);
      if (i % 10_000 == 0) {
        System.out.println("flushing");
        for (int w = 0; w < N; w++) {
          writers[w].flush();
        }
      }
    }
    System.out.println("closing");
    for (int w = 0; w < N; w++) {
      writers[w].close();
    }
    System.out.println("stopping the agitator");
    stop.set(true);
    agitator.join();
    System.out.println("checking the data");
    long count = 0;
    for (int w = 0; w < N; w++) {
      try (Scanner scanner = c.createScanner(tables[w], Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : scanner) {
          int value = Integer.parseInt(entry.getValue().toString());
          assertEquals(w, value);
          count++;
        }
      }
    }
    assertEquals(1_000_000, count);
  }

  private Thread agitator(final AtomicBoolean stop) {
    return new Thread() {
      @Override
      public void run() {
        try {
          int i = 0;
          while (!stop.get()) {
            sleepUninterruptibly(10, TimeUnit.SECONDS);
            System.out.println("Restarting");
            getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);
            getCluster().start();
            // read the metadata table to know everything is back up
            Iterators.size(getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY).iterator());
            i++;
          }
          System.out.println("Restarted " + i + " times");
        } catch (Exception ex) {
          log.error("{}", ex.getMessage(), ex);
        }
      }
    };
  }
}
