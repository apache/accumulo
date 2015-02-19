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
import static org.junit.Assert.assertTrue;

import java.util.Map.Entry;
import java.util.Random;
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
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

public class MultiTableRecoveryIT extends ConfigurableMacIT {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");

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
    for (i = 0; i < 1000 * 1000; i++) {
      long randomRow = Math.abs(random.nextLong());
      assertTrue(randomRow >= 0);
      final int table = (int) (randomRow % N);
      final Mutation m = new Mutation(Long.toHexString(randomRow));
      m.put(new byte[0], new byte[0], values[table]);
      writers[table].addMutation(m);
      if ((i % (10 * 1000)) == 0) {
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
      Scanner scanner = c.createScanner(tables[w], Authorizations.EMPTY);
      for (Entry<Key,Value> entry : scanner) {
        int value = Integer.parseInt(entry.getValue().toString());
        assertEquals(w, value);
        count++;
      }
      scanner.close();
    }
    assertEquals(1000 * 1000, count);
  }

  private Thread agitator(final AtomicBoolean stop) {
    return new Thread() {
      @Override
      public void run() {
        try {
          int i = 0;
          while (!stop.get()) {
            UtilWaitThread.sleep(5 * 1000);
            System.out.println("Restarting");
            for (ProcessReference proc : getCluster().getProcesses().get(ServerType.TABLET_SERVER)) {
              getCluster().killProcess(ServerType.TABLET_SERVER, proc);
            }
            getCluster().start();
            // read the metadata table to know everything is back up
            for (@SuppressWarnings("unused")
            Entry<Key,Value> unused : getConnector().createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {}
            i++;
          }
          System.out.println("Restarted " + i + " times");
        } catch (Exception ex) {
          log.error(ex, ex);
        }
      }
    };
  }
}
