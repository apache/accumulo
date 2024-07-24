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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ScanServerMetadataEntries;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerMetadataEntriesCleanIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void start() throws Exception {
    startMiniCluster();
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
        .map(f -> new ScanServerRefTabletFile(serverLockUUID, server.toString(), f))
        .collect(Collectors.toSet());

    ServerContext ctx = getCluster().getServerContext();

    ctx.getAmple().putScanServerFileReferences(scanRefs);
    assertEquals(scanRefs.size(), ctx.getAmple().getScanServerFileReferences().count());

    Set<ScanServerRefTabletFile> scanRefs2 =
        ctx.getAmple().getScanServerFileReferences().collect(Collectors.toSet());
    assertEquals(scanRefs, scanRefs2);

    ScanServerMetadataEntries.clean(ctx);
    assertFalse(ctx.getAmple().getScanServerFileReferences().findAny().isPresent());
  }

  @Test
  public void testOldScanServerRefs() {
    HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
    UUID serverLockUUID = UUID.randomUUID();

    Set<ScanServerRefTabletFile> scanRefs = Stream.of("F0001270.rf", "F0001271.rf")
        .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/test_tablet/" + f)
        .map(f -> new ScanServerRefTabletFile(serverLockUUID, server.toString(), f))
        .collect(Collectors.toSet());

    ServerContext ctx = getCluster().getServerContext();
    ctx.getAmple().putScanServerFileReferences(scanRefs);

    assertEquals(scanRefs.size(), ctx.getAmple().getScanServerFileReferences().count());

    // Add old scan server entries
    try (BatchWriter writer = ctx.createBatchWriter(Ample.DataLevel.USER.metaTable())) {
      @SuppressWarnings("deprecation")
      String prefix =
          org.apache.accumulo.core.metadata.schema.MetadataSchema.OldScanServerFileReferenceSection
              .getRowPrefix();
      for (String filepath : Stream.of("F0001243.rf", "F0006512.rf")
          .map(f -> "hdfs://localhost:8020/accumulo/tables/2a/test_tablet/" + f)
          .collect(Collectors.toSet())) {
        Mutation m = new Mutation(prefix + filepath);
        m.put(server.toString(), serverLockUUID.toString(), "");
        writer.addMutation(m);
      }
      writer.flush();
    } catch (MutationsRejectedException | TableNotFoundException e) {
      throw new IllegalStateException(
          "Error inserting scan server file references into " + Ample.DataLevel.USER.metaTable(),
          e);
    }

    // Ensure that ample returns all references from both ranges
    assertEquals(scanRefs.size() + 2, ctx.getAmple().getScanServerFileReferences().count());

    // Delete all references
    ctx.getAmple().deleteScanServerFileReferences(
        ctx.getAmple().getScanServerFileReferences().collect(Collectors.toSet()));
    assertEquals(0, ctx.getAmple().getScanServerFileReferences().count());
  }
}
