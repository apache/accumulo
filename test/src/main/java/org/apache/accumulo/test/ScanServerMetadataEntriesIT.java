/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ScanServerFileReferenceSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.gc.GarbageCollectionEnvironment.Reference;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.gc.SimpleGarbageCollector.GCEnv;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerMetadataEntriesIT extends SharedMiniClusterBase {

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
  public void testServerContextMethods() throws Exception {

    try (AccumuloClient ac = Accumulo.newClient().from(getClientProps()).build()) {
      HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
      UUID serverLockUUID = UUID.randomUUID();

      String[] files =
          new String[] {"hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf",
              "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000071.rf"};

      Set<ScanServerRefTabletFile> scanRefs = new HashSet<>();
      for (String file : files) {
        scanRefs.add(new ScanServerRefTabletFile(file, server.toString(), serverLockUUID));
      }

      ServerContext ctx = getCluster().getServerContext();

      ctx.getAmple().putScanServerFileReferences(scanRefs);
      assertEquals(2, ctx.getAmple().getScanServerFileReferences().count());

      Set<ScanServerRefTabletFile> scanRefs2 =
          ctx.getAmple().getScanServerFileReferences().collect(Collectors.toSet());

      assertEquals(scanRefs, scanRefs2);

      ctx.getAmple().deleteScanServerFileReferences("127.0.0.1:1234", serverLockUUID);
      assertEquals(0, ctx.getAmple().getScanServerFileReferences().count());

      ctx.getAmple().putScanServerFileReferences(scanRefs);
      assertEquals(2, ctx.getAmple().getScanServerFileReferences().count());

      ctx.getAmple().deleteScanServerFileReferences(scanRefs);
      assertEquals(0, ctx.getAmple().getScanServerFileReferences().count());

    }
  }

  @Test
  public void testScanServerMetadataEntries() throws Exception {

    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      // Make multiple files
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setBatchSize(10);

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());

        Thread.sleep(6000); // wait twice the insert interval

        assertEquals(3, ctx.getAmple().getScanServerFileReferences().count());

      }

      // close happens asynchronously. Let the test fail by timeout
      long count = ctx.getAmple().getScanServerFileReferences().count();
      while (count != 0) {
        Thread.sleep(1000);
        count = ctx.getAmple().getScanServerFileReferences().count();
      }
    }
  }

  @Test
  public void testBatchScanServerMetadataEntries() throws Exception {

    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      // Make multiple files
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);

      try (BatchScanner scanner = client.createBatchScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRanges(Collections.singletonList(new Range()));
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());

        assertEquals(3, ctx.getAmple().getScanServerFileReferences().count());

      }

      // close happens asynchronously. Let the test fail by timeout
      long count = ctx.getAmple().getScanServerFileReferences().count();
      while (count != 0) {
        Thread.sleep(1000);
        count = ctx.getAmple().getScanServerFileReferences().count();
      }
    }
  }

  @Test
  public void testGcEnvScanServerReferences() throws Exception {

    @SuppressWarnings("resource")
    GCEnv gc = new SimpleGarbageCollector(new ServerOpts(),
        new String[] {"-p", getCluster().getAccumuloPropertiesPath()}).new GCEnv(DataLevel.USER);

    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);
      TableId tid = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));

      // Make multiple files
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);
      ReadWriteIT.ingest(client, getClientInfo(), 10, 10, 50, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);

      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setRange(new Range());
        scanner.setConsistencyLevel(ConsistencyLevel.EVENTUAL);
        scanner.setBatchSize(10);

        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        assertTrue(iter.hasNext());
        assertNotNull(iter.next());

        List<Entry<Key,Value>> metadataEntries = null;
        try (Scanner scanner2 = client.createScanner("accumulo.metadata", Authorizations.EMPTY)) {
          scanner2.setRange(ScanServerFileReferenceSection.getRange());
          metadataEntries = StreamSupport.stream(scanner2.spliterator(), false).distinct()
              .collect(Collectors.toList());
        }
        assertEquals(3, metadataEntries.size());
        metadataEntries.forEach(
            e -> LoggerFactory.getLogger(ScanServerMetadataEntriesIT.class).info("{}", e.getKey()));

        Set<String> metadataScanFileRefs = new HashSet<>();
        metadataEntries.forEach(m -> {
          String row = m.getKey().getRow().toString();
          assertTrue(row.startsWith("~sserv"));
          String file = row.substring(ScanServerFileReferenceSection.getRowPrefix().length());
          metadataScanFileRefs.add(file);
        });
        assertEquals(3, metadataScanFileRefs.size());

        Set<Reference> refs = gc.getReferences().collect(Collectors.toSet());
        List<Reference> tableRefs = new ArrayList<>();
        refs.forEach(r -> {
          if (r.id.equals(tid) && !r.isDir) {
            assertTrue(metadataScanFileRefs.contains(r.ref));
            tableRefs.add(r);
          }
        });
        LoggerFactory.getLogger(ScanServerMetadataEntriesIT.class).info("{}", tableRefs);
        // There should be 6 references here. 3 for the table file entries, and 3 for the scan
        // server references
        assertEquals(6, tableRefs.size());

        TreeSet<Reference> deduplicatedReferences = new TreeSet<>(new Comparator<Reference>() {
          @Override
          public int compare(Reference r1, Reference r2) {
            if (r1 == r2) {
              return 0;
            }
            int ret = r1.id.compareTo(r2.id);
            if (ret == 0) {
              ret = Boolean.compare(r1.isDir, r2.isDir);
              if (ret == 0) {
                return r1.ref.compareTo(r2.ref);
              } else {
                return ret;
              }
            } else {
              return ret;
            }
          }
        });
        deduplicatedReferences.addAll(tableRefs);
        assertEquals(3, deduplicatedReferences.size());
        deduplicatedReferences.forEach(ddr -> assertTrue(metadataScanFileRefs.contains(ddr.ref)));
      }

      client.tableOperations().delete(tableName);
    }
    // close happens asynchronously. Let the test fail by timeout
    long count = ctx.getAmple().getScanServerFileReferences().count();
    while (count != 0) {
      Thread.sleep(1000);
      count = ctx.getAmple().getScanServerFileReferences().count();
    }

  }

}
