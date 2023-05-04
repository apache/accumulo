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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ErrorThrowingIterator;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

@Tag(MINI_CLUSTER_ONLY)
public class IteratorThrowingErrorIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(IteratorThrowingErrorIT.class);

  private static class TestConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumTservers(1);

      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "5s");

      // Configure the scan server to only have 1 scan executor thread. This means
      // that the scan server will run scans serially, not concurrently.
      cfg.setProperty(Property.SSERV_SCAN_EXECUTORS_DEFAULT_THREADS, "1");
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    TestConfiguration c = new TestConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);

    String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    String scanServerRoot = zooRoot + Constants.ZTSERVERS;

    while (zrw.getChildren(scanServerRoot).size() < 1) {
      LOG.debug("waiting for tservers to register");
      Thread.sleep(500);
    }
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testScanGoPath() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = createTableAndIngest(client, tableName, null, 10, 10, "colf");

      IteratorSetting is = new IteratorSetting(10, ErrorThrowingIterator.class);

      client.tableOperations().attachIterator(tableName, is,
          EnumSet.of(IteratorUtil.IteratorScope.scan, IteratorUtil.IteratorScope.minc,
              IteratorUtil.IteratorScope.majc));

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        assertEquals(ingestedEntryCount, Iterables.size(scanner),
            "The scan server scanner should have seen all ingested and flushed entries");
      } // when the scanner is closed, all open sessions should be closed
    }
  }

  @Test
  public void testErrorOnNext() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = createTableAndIngest(client, tableName, null, 10, 10, "colf");

      IteratorSetting is = new IteratorSetting(10, ErrorThrowingIterator.class);
      ErrorThrowingIterator.setThrowErrorOnNext(is);

      client.tableOperations().attachIterator(tableName, is,
          EnumSet.of(IteratorUtil.IteratorScope.scan, IteratorUtil.IteratorScope.minc,
              IteratorUtil.IteratorScope.majc));

      assertThrows(RuntimeException.class, () -> {
        try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRange(new Range());
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          assertEquals(ingestedEntryCount, Iterables.size(scanner),
              "The scan server scanner should have seen all ingested and flushed entries");
        } // when the scanner is closed, all open sessions should be closed
      });
    }
    scanMetadata();
  }

  @Test
  public void testErrorOnSeek() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = createTableAndIngest(client, tableName, null, 10, 10, "colf");

      IteratorSetting is = new IteratorSetting(10, ErrorThrowingIterator.class);
      ErrorThrowingIterator.setThrowErrorOnSeek(is);

      client.tableOperations().attachIterator(tableName, is,
          EnumSet.of(IteratorUtil.IteratorScope.scan, IteratorUtil.IteratorScope.minc,
              IteratorUtil.IteratorScope.majc));

      assertThrows(RuntimeException.class, () -> {
        try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRange(new Range());
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          assertEquals(ingestedEntryCount, Iterables.size(scanner),
              "The scan server scanner should have seen all ingested and flushed entries");
        } // when the scanner is closed, all open sessions should be closed
      });
    }
    scanMetadata();
  }

  @Test
  public void testErrorOnInit() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      final int ingestedEntryCount = createTableAndIngest(client, tableName, null, 10, 10, "colf");

      IteratorSetting is = new IteratorSetting(10, ErrorThrowingIterator.class);
      ErrorThrowingIterator.setThrowErrorOnInit(is);

      client.tableOperations().attachIterator(tableName, is,
          EnumSet.of(IteratorUtil.IteratorScope.scan, IteratorUtil.IteratorScope.minc,
              IteratorUtil.IteratorScope.majc));

      assertThrows(RuntimeException.class, () -> {
        try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
          scanner.setRange(new Range());
          scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
          assertEquals(ingestedEntryCount, Iterables.size(scanner),
              "The scan server scanner should have seen all ingested and flushed entries");
        } // when the scanner is closed, all open sessions should be closed
      });
    }
    scanMetadata();
  }

  /**
   * Create a table with the given name and the given client. Then, ingest into the table using
   * {@link #ingest(AccumuloClient, String, int, int, int, String, boolean)}
   *
   * @param client used to create the table
   * @param tableName used to create the table
   * @param ntc used to create the table. if null, a new NewTableConfiguration will replace it
   * @param rowCount number of rows to ingest
   * @param colCount number of columns to ingest
   * @param colf column family to use for ingest
   * @return the number of ingested entries
   */
  protected static int createTableAndIngest(AccumuloClient client, String tableName,
      NewTableConfiguration ntc, int rowCount, int colCount, String colf) throws Exception {

    if (Objects.isNull(ntc)) {
      ntc = new NewTableConfiguration();
    }

    client.tableOperations().create(tableName, ntc);

    return ingest(client, tableName, rowCount, colCount, 0, colf, true);
  }

  /**
   * Ingest into the table using the given parameters, then optionally flush the table
   *
   * @param client used to create the table
   * @param tableName used to create the table
   * @param rowCount number of rows to ingest
   * @param colCount number of columns to ingest
   * @param offset the offset to use for ingest
   * @param colf column family to use for ingest
   * @param shouldFlush if true, the entries will be flushed after ingest
   * @return the number of ingested entries
   */
  protected static int ingest(AccumuloClient client, String tableName, int rowCount, int colCount,
      int offset, String colf, boolean shouldFlush) throws Exception {
    ReadWriteIT.ingest(client, colCount, rowCount, 50, offset, colf, tableName);

    final int ingestedEntriesCount = colCount * rowCount;

    if (shouldFlush) {
      client.tableOperations().flush(tableName, null, null, true);
    }

    return ingestedEntriesCount;
  }

  private void scanMetadata() {
    // scan metadata to validate tserver still online.
    Ample ample = getCluster().getServerContext().getAmple();
    AtomicInteger count = new AtomicInteger(0);
    try (TabletsMetadata tablets =
        ample.readTablets().forTable(MetadataTable.ID).fetch(PREV_ROW, FILES).build()) {
      tablets.forEach(tm -> {
        count.incrementAndGet();
        LOG.warn("TM: {}", tm.getExtent());
        tm.getFiles().forEach(
            f -> LOG.warn("METADATA Scan (tid=1) E: {}, P: {}", tm.getExtent(), f.getPath()));
      });
    }
    assertTrue(count.get() > 0);
  }
}
