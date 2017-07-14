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
package org.apache.accumulo.test.performance;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
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
public class RollWALPerformanceIT extends ConfigurableMacBase {

  @BeforeClass
  static public void checkMR() {
    assumeFalse(IntegrationTestMapReduce.isMapReduce());
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_WAL_REPLICATION, "1");
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "5M");
    cfg.setProperty(Property.TABLE_MINC_LOGS_MAX, "100");
    cfg.setProperty(Property.GC_FILE_ARCHIVE, "false");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.useMiniDFS(true);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 5 * 60;
  }

  private long ingest() throws Exception {
    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];

    log.info("Creating the table");
    c.tableOperations().create(tableName);

    log.info("Splitting the table");
    final long SPLIT_COUNT = 100;
    final long distance = Long.MAX_VALUE / SPLIT_COUNT;
    final SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < SPLIT_COUNT; i++) {
      splits.add(new Text(String.format("%016x", i * distance)));
    }
    c.tableOperations().addSplits(tableName, splits);

    log.info("Waiting for balance");
    c.instanceOperations().waitForBalance();

    final Instance inst = c.getInstance();

    log.info("Starting ingest");
    final long start = System.nanoTime();
    final String args[] = {"-i", inst.getInstanceName(), "-z", inst.getZooKeepers(), "-u", "root", "-p", ROOT_PASSWORD, "--batchThreads", "2", "--table",
        tableName, "--num", Long.toString(50 * 1000), // 50K 100 byte entries
    };

    ContinuousIngest.main(args);
    final long result = System.nanoTime() - start;
    log.debug(String.format("Finished in %,d ns", result));
    log.debug("Dropping table");
    c.tableOperations().delete(tableName);
    return result;
  }

  private long getAverage() throws Exception {
    final int REPEAT = 3;
    long totalTime = 0;
    for (int i = 0; i < REPEAT; i++) {
      totalTime += ingest();
    }
    return totalTime / REPEAT;
  }

  @Test
  public void testWalPerformanceOnce() throws Exception {
    // get time with a small WAL, which will cause many WAL roll-overs
    long avg1 = getAverage();
    // use a bigger WAL max size to eliminate WAL roll-overs
    Connector c = getConnector();
    c.instanceOperations().setProperty(Property.TSERV_WALOG_MAX_SIZE.getKey(), "1G");
    c.tableOperations().flush(MetadataTable.NAME, null, null, true);
    c.tableOperations().flush(RootTable.NAME, null, null, true);
    getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);
    getCluster().start();
    long avg2 = getAverage();
    log.info(String.format("Average run time with small WAL %,d with large WAL %,d", avg1, avg2));
    assertTrue(avg1 > avg2);
    double percent = (100. * avg1) / avg2;
    log.info(String.format("Percent of large log: %.2f%%", percent));
  }

}
