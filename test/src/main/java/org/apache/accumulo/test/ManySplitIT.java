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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.categories.PerformanceTests;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.mrit.IntegrationTestMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiniClusterOnlyTests.class, PerformanceTests.class})
public class ManySplitIT extends ConfigurableMacBase {

  final int SPLITS = 10_000;

  @BeforeClass
  static public void checkMR() {
    assumeFalse(IntegrationTestMapReduce.isMapReduce());
  }

  @Test(timeout = 4 * 60 * 1000)
  public void test() throws Exception {
    assumeFalse(IntegrationTestMapReduce.isMapReduce());

    final String tableName = getUniqueNames(1)[0];

    log.info("Creating table");
    final TableOperations tableOperations = getConnector().tableOperations();

    log.info("splitting metadata table");
    tableOperations.create(tableName);
    SortedSet<Text> splits = new TreeSet<>();
    for (byte b : "123456789abcde".getBytes(UTF_8)) {
      splits.add(new Text(new byte[] {'1', ';', b}));
    }
    tableOperations.addSplits(MetadataTable.NAME, splits);
    splits.clear();
    for (int i = 0; i < SPLITS; i++) {
      splits.add(new Text(Integer.toHexString(i)));
    }
    log.info("Adding splits");
    // print out the number of splits so we have some idea of what's going on
    final AtomicBoolean stop = new AtomicBoolean(false);
    Thread t = new Thread() {
      @Override
      public void run() {
        while (!stop.get()) {
          UtilWaitThread.sleep(1000);
          try {
            log.info("splits: " + tableOperations.listSplits(tableName).size());
          } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    };
    t.start();
    long now = System.currentTimeMillis();
    tableOperations.addSplits(tableName, splits);
    long diff = System.currentTimeMillis() - now;
    double splitsPerSec = SPLITS / (diff / 1000.);
    log.info("Done: {} splits per second", splitsPerSec);
    assertTrue("splits created too slowly", splitsPerSec > 100);
    stop.set(true);
    t.join();
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hdfs) {
    cfg.setNumTservers(1);
    cfg.setMemory(ServerType.TABLET_SERVER, cfg.getMemory(ServerType.TABLET_SERVER) * 2, MemoryUnit.BYTE);
  }

}
