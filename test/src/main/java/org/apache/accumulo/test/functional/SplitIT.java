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

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.CheckForMetadataProblems;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SplitIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(SplitIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAXMEM, "5K");
    cfg.setMemory(ServerType.TABLET_SERVER, 384, MemoryUnit.MEGABYTE);
  }

  private String tservMaxMem, tservMajcDelay;

  @BeforeEach
  public void alterConfig() throws Exception {
    assumeTrue(getClusterType() == ClusterType.MINI);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();
      Map<String,String> config = iops.getSystemConfiguration();
      tservMaxMem = config.get(Property.TSERV_MAXMEM.getKey());

      // Property.TSERV_MAXMEM can't be altered on a running server
      boolean restarted = false;
      if (!tservMaxMem.equals("5K")) {
        iops.setProperty(Property.TSERV_MAXMEM.getKey(), "5K");
        getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
        restarted = true;
      }
    }
  }

  @AfterEach
  public void resetConfig() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      if (tservMaxMem != null) {
        log.info("Resetting {}={}", Property.TSERV_MAXMEM.getKey(), tservMaxMem);
        client.instanceOperations().setProperty(Property.TSERV_MAXMEM.getKey(), tservMaxMem);
        tservMaxMem = null;
        getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
        getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      }
    }
  }

  @Test
  public void tabletShouldSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "256K");
      props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");

      c.tableOperations().create(table, new NewTableConfiguration().setProperties(props));
      VerifyParams params = new VerifyParams(getClientProps(), table, 100_000);
      TestIngest.ingest(c, params);
      VerifyIngest.verifyIngest(c, params);
      while (c.tableOperations().listSplits(table).size() < 10) {
        Thread.sleep(SECONDS.toMillis(15));
      }
      TableId id = TableId.of(c.tableOperations().tableIdMap().get(table));
      try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        KeyExtent extent = new KeyExtent(id, null, null);
        s.setRange(extent.toMetaRange());
        TabletColumnFamily.PREV_ROW_COLUMN.fetch(s);
        int count = 0;
        int shortened = 0;
        for (Entry<Key,Value> entry : s) {
          extent = KeyExtent.fromMetaPrevRow(entry);
          if (extent.endRow() != null && extent.endRow().toString().length() < 14) {
            shortened++;
          }
          count++;
        }

        assertTrue(shortened > 0, "Shortened should be greater than zero: " + shortened);
        assertTrue(count > 10, "Count should be greater than 10: " + count);
      }

      assertEquals(0, getCluster().getClusterControl().exec(CheckForMetadataProblems.class,
          new String[] {"-c", cluster.getClientPropsPath()}));
    }
  }

  @Test
  public void interleaveSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
      props.put(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");

      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      Thread.sleep(SECONDS.toMillis(5));
      ReadWriteIT.interleaveTest(c, tableName);
      Thread.sleep(SECONDS.toMillis(5));
      int numSplits = c.tableOperations().listSplits(tableName).size();
      while (numSplits <= 20) {
        log.info("Waiting for splits to happen");
        Thread.sleep(2000);
        numSplits = c.tableOperations().listSplits(tableName).size();
      }
      assertTrue(numSplits > 20, "Expected at least 20 splits, saw " + numSplits);
    }
  }

  @Test
  public void deleteSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName,
          new NewTableConfiguration().setProperties(Map.of(Property.TABLE_SPLIT_THRESHOLD.getKey(),
              "10K", Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K")));
      DeleteIT.deleteTest(c, getCluster(), tableName);
      c.tableOperations().flush(tableName, null, null, true);
      for (int i = 0; i < 5; i++) {
        Thread.sleep(SECONDS.toMillis(10));
        if (c.tableOperations().listSplits(tableName).size() > 20) {
          break;
        }
      }
      assertTrue(c.tableOperations().listSplits(tableName).size() > 20);
    }
  }

  @Test
  public void testLargeSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName, new NewTableConfiguration()
          .setProperties(Map.of(Property.TABLE_MAX_END_ROW_SIZE.getKey(), "10K")));

      byte[] okSplit = new byte[4096];
      for (int i = 0; i < okSplit.length; i++) {
        okSplit[i] = (byte) (i % 256);
      }

      var splits1 = new TreeSet<Text>(List.of(new Text(okSplit)));

      c.tableOperations().addSplits(tableName, splits1);

      assertEquals(splits1, new TreeSet<>(c.tableOperations().listSplits(tableName)));

      byte[] bigSplit = new byte[4096 * 4];
      for (int i = 0; i < bigSplit.length; i++) {
        bigSplit[i] = (byte) (i % 256);
      }

      var splits2 = new TreeSet<Text>(List.of(new Text(bigSplit)));
      // split should fail because it exceeds the configured max split size
      assertThrows(AccumuloException.class,
          () -> c.tableOperations().addSplits(tableName, splits2));

      // ensure the large split is not there
      assertEquals(splits1, new TreeSet<>(c.tableOperations().listSplits(tableName)));
    }
  }

  @Test
  public void concurrentSplit() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      final String tableName = getUniqueNames(1)[0];

      log.debug("Creating table {}", tableName);
      c.tableOperations().create(tableName);

      final int numRows = 100_000;
      log.debug("Ingesting {} rows into {}", numRows, tableName);
      VerifyParams params = new VerifyParams(getClientProps(), tableName, numRows);
      TestIngest.ingest(c, params);

      log.debug("Verifying {} rows ingested into {}", numRows, tableName);
      VerifyIngest.verifyIngest(c, params);

      log.debug("Creating futures that add random splits to the table");
      ExecutorService es = Executors.newFixedThreadPool(10);
      final int totalFutures = 100;
      final int splitsPerFuture = 4;
      final Set<Text> totalSplits = new HashSet<>();
      List<Callable<Void>> tasks = new ArrayList<>(totalFutures);
      for (int i = 0; i < totalFutures; i++) {
        final Pair<Integer,Integer> splitBounds = getRandomSplitBounds(numRows);
        final TreeSet<Text> splits = TestIngest.getSplitPoints(splitBounds.getFirst().longValue(),
            splitBounds.getSecond().longValue(), splitsPerFuture);
        totalSplits.addAll(splits);
        tasks.add(() -> {
          c.tableOperations().addSplits(tableName, splits);
          return null;
        });
      }

      log.debug("Submitting futures");
      List<Future<Void>> futures =
          tasks.parallelStream().map(es::submit).collect(Collectors.toList());

      log.debug("Waiting for futures to complete");
      for (Future<?> f : futures) {
        f.get();
      }
      es.shutdown();

      log.debug("Checking that {} splits were created ", totalSplits.size());

      assertEquals(totalSplits, new HashSet<>(c.tableOperations().listSplits(tableName)),
          "Did not see expected splits");

      log.debug("Verifying {} rows ingested into {}", numRows, tableName);
      VerifyIngest.verifyIngest(c, params);
    }
  }

  /**
   * Generates a pair of integers that represent the start and end of a range of splits. The start
   * and end are randomly generated between 0 and upperBound. The start is guaranteed to be less
   * than the end and the two bounds are guaranteed to be different values.
   *
   * @param upperBound the upper bound of the range of splits
   * @return a pair of integers that represent the start and end of a range of splits
   */
  private Pair<Integer,Integer> getRandomSplitBounds(int upperBound) {
    Preconditions.checkArgument(upperBound > 1, "upperBound must be greater than 1");

    int start = RANDOM.get().nextInt(upperBound);
    int end = RANDOM.get().nextInt(upperBound - 1);

    // ensure start is less than end and that end is not equal to start
    if (end >= start) {
      end += 1;
    } else {
      int tmp = start;
      start = end;
      end = tmp;
    }

    return new Pair<>(start, end);
  }

  private String getDir() throws Exception {
    var rootPath = getCluster().getTemporaryPath().toString();
    String dir = rootPath + "/" + getUniqueNames(1)[0];
    getCluster().getFileSystem().delete(new Path(dir), true);
    return dir;
  }

  @SuppressFBWarnings(value = {"PREDICTABLE_RANDOM", "DMI_RANDOM_USED_ONLY_ONCE"},
      justification = "predictable random with specific seed is intended for this test")
  @Test
  public void bulkImportThatCantSplitHangsCompaction() throws Exception {

    /*
     * There was a bug where a bulk import into a tablet with the following conditions would cause
     * compactions to hang.
     *
     * 1. Tablet where the files sizes indicates its needs to split
     *
     * 2. Row with many columns in the tablet that is unsplittable
     *
     * This happened because the bulk import plus an attempted split would leave the tablet in a bad
     * internal state for compactions.
     */

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      c.tableOperations().create(tableName, new NewTableConfiguration()
          .setProperties(singletonMap(Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K")));

      Random random = new Random();
      byte[] val = new byte[100];

      String dir = getDir();
      String file = dir + "/f1.rf";

      // create a file with a single row and lots of columns. The files size will exceed the split
      // threshold configured above.
      try (
          RFileWriter writer = RFile.newWriter().to(file).withFileSystem(getFileSystem()).build()) {
        writer.startDefaultLocalityGroup();
        for (int i = 0; i < 1000; i++) {
          random.nextBytes(val);
          writer.append(new Key("r1", "f1", String.format("%09d", i)),
              new Value(Base64.getEncoder().encodeToString(val)));
        }
      }

      // import the file
      c.tableOperations().importDirectory(dir).to(tableName).load();

      // tablet should not be able to split
      assertEquals(0, c.tableOperations().listSplits(tableName).size());

      Thread.sleep(1000);

      c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      // should have over 100K of data in the values
      assertTrue(
          c.createScanner(tableName).stream().mapToLong(entry -> entry.getValue().getSize()).sum()
              > 100_000);

      // should have 1000 entries
      assertEquals(1000, c.createScanner(tableName).stream().count());
    }
  }
}
