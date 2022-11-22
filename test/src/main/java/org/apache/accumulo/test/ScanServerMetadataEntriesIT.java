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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.test.ScanServerIT.ingest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ScanServerFileReferenceSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.gc.GCRun;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerMetadataEntriesIT extends SharedMiniClusterBase {

  public static final Logger log = LoggerFactory.getLogger(ScanServerMetadataEntriesIT.class);

  private static class ScanServerMetadataEntriesITConfiguration
      implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.setNumScanServers(1);
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");
      cfg.setProperty(Property.SSERVER_SCAN_REFERENCE_EXPIRATION_TIME, "5s");
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    ScanServerMetadataEntriesITConfiguration c = new ScanServerMetadataEntriesITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
    SharedMiniClusterBase.getCluster().getClusterControl().start(ServerType.SCAN_SERVER,
        "localhost");

    String zooRoot = getCluster().getServerContext().getZooKeeperRoot();
    ZooReaderWriter zrw = getCluster().getServerContext().getZooReaderWriter();
    String scanServerRoot = zooRoot + Constants.ZSSERVERS;

    while (zrw.getChildren(scanServerRoot).size() == 0) {
      Thread.sleep(500);
    }
  }

  @AfterAll
  public static void stop() throws Exception {
    stopMiniCluster();
  }

  @Test
  public void testServerContextMethods() {
    HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
    UUID serverLockUUID = UUID.randomUUID();

    Set<ScanServerRefTabletFile> scanRefs = Stream.of("F0000070.rf", "F0000071.rf")
        .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/" + f)
        .map(f -> new ScanServerRefTabletFile(f, server.toString(), serverLockUUID))
        .collect(Collectors.toSet());

    ServerContext ctx = getCluster().getServerContext();

    ctx.getAmple().putScanServerFileReferences(scanRefs);
    assertEquals(scanRefs.size(), ctx.getAmple().getScanServerFileReferences().count());

    Set<ScanServerRefTabletFile> scanRefs2 =
        ctx.getAmple().getScanServerFileReferences().collect(Collectors.toSet());

    assertEquals(scanRefs, scanRefs2);

    // attempt to delete file references then make sure they were deleted
    ctx.getAmple().deleteScanServerFileReferences(server.toString(), serverLockUUID);
    assertFalse(ctx.getAmple().getScanServerFileReferences().findAny().isPresent());

    ctx.getAmple().putScanServerFileReferences(scanRefs);
    assertEquals(scanRefs.size(), ctx.getAmple().getScanServerFileReferences().count());

    // attempt to delete file references then make sure they were deleted
    ctx.getAmple().deleteScanServerFileReferences(scanRefs);
    assertFalse(ctx.getAmple().getScanServerFileReferences().findAny().isPresent());
  }

  @Test
  public void testScanServerMetadataEntries() throws Exception {

    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      // Make multiple files
      final int fileCount = 3;
      for (int i = 0; i < fileCount; i++) {
        ingest(client, tableName, 10, 10, 0, "colf", true);
      }

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setBatchSize(10);

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());

        assertEquals(fileCount, ctx.getAmple().getScanServerFileReferences().count());

      }

      // close happens asynchronously. Let the test fail by timeout
      while (ctx.getAmple().getScanServerFileReferences().findAny().isPresent()) {
        Thread.sleep(1000);
      }
    }
  }

  @Test
  public void testBatchScanServerMetadataEntries() throws Exception {

    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      // Make multiple files
      final int fileCount = 3;
      for (int i = 0; i < fileCount; i++) {
        ingest(client, tableName, 10, 10, 0, "colf", true);
      }

      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singletonList(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());

        assertEquals(fileCount, ctx.getAmple().getScanServerFileReferences().count());

      }

      // close happens asynchronously. Let the test fail by timeout
      while (ctx.getAmple().getScanServerFileReferences().findAny().isPresent()) {
        Thread.sleep(1000);
      }
    }
  }

  @Test
  public void testGcRunScanServerReferences() throws Exception {

    ServerContext ctx = getCluster().getServerContext();
    GCRun gc = new GCRun(DataLevel.USER, ctx);
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      TableId tid = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));

      // Make multiple files
      final int fileCount = 3;
      for (int i = 0; i < fileCount; i++) {
        ingest(client, tableName, 10, 10, 0, "colf", true);
      }

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setBatchSize(10);

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());

        List<Entry<Key,Value>> metadataEntries = null;
        try (Scanner scanner2 = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
          scanner2.setRange(ScanServerFileReferenceSection.getRange());
          metadataEntries = scanner2.stream().distinct().collect(Collectors.toList());
        }
        assertEquals(fileCount, metadataEntries.size());
        metadataEntries.forEach(e -> log.info("{}", e.getKey()));

        Set<String> metadataScanFileRefs = new HashSet<>();
        metadataEntries.forEach(m -> {
          String row = m.getKey().getRow().toString();
          assertTrue(row.startsWith("~sserv"));
          String file = row.substring(ScanServerFileReferenceSection.getRowPrefix().length());
          metadataScanFileRefs.add(file);
        });
        assertEquals(fileCount, metadataScanFileRefs.size());

        assertEquals(fileCount, ctx.getAmple().getScanServerFileReferences().count());

        List<Reference> refs = gc.getReferences().collect(Collectors.toList());
        assertTrue(refs.size() > fileCount * 2);
        List<Reference> tableRefs =
            refs.stream().filter(r -> r.getTableId().equals(tid) && !r.isDirectory())
                .peek(r -> assertTrue(metadataScanFileRefs.contains(r.getMetadataEntry())))
                .collect(Collectors.toList());
        log.info("Reference List:{}", tableRefs);
        // There should be 6 references here. 3 for the table file entries, and 3 for the scan
        // server references
        assertEquals(fileCount * 2, tableRefs.size());

        Set<String> deduplicatedReferences =
            tableRefs.stream().map(Reference::getMetadataEntry).collect(Collectors.toSet());

        assertEquals(fileCount, deduplicatedReferences.size());
      }

      client.tableOperations().delete(tableName);
    }
    // close happens asynchronously. Let the test fail by timeout
    while (ctx.getAmple().getScanServerFileReferences().findAny().isPresent()) {
      Thread.sleep(1000);
    }

  }

}
