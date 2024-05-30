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
package org.apache.accumulo.test.ample.usage;

import static org.apache.accumulo.core.client.ConditionalWriter.Status.ACCEPTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.UNKNOWN;
import static org.apache.accumulo.test.ample.metadata.ConditionalWriterInterceptor.withStatus;
import static org.apache.accumulo.tserver.tablet.Tablet.updateTabletDataFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.tablets.TabletTime;
import org.apache.accumulo.test.ample.metadata.TestAmple;
import org.apache.accumulo.tserver.MinorCompactionReason;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests tablet usage of ample to add a new minor compacted file to tablet. This tests edge cases,
 * the normal cases are well tested by many other ITs from simply running Accumulo.
 */
public class TabletFileUpdateIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static final ReferencedTabletFile newFile =
      ReferencedTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/F1.rf"));
  private static final DataFileValue dfv1 = new DataFileValue(1000, 100);
  private static final TServerInstance tserverInstance =
      new TServerInstance("localhost:9997", 0xabcdef123L);
  private static final TableId tableId = TableId.of("99");
  private static final KeyExtent extent = new KeyExtent(tableId, null, null);

  /**
   * This test ensures that a tablet handles tablet time changing externally (like bulk import could
   * change tablet time).
   */
  @Test
  public void testTimeChanged() throws Exception {
    String[] tableNames = getUniqueNames(1);
    String metadataTable = tableNames[0];

    var tabletTime = TabletTime.getInstance(new MetadataTime(1, TimeType.LOGICAL));
    long flushNonce = 42L;

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(metadataTable);
      TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable));

      Ample testAmple = TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable));

      assertNull(testAmple.readTablet(extent));

      // create tablet in the fake metadata table
      testAmple.mutateTablet(extent).putDirName("dir1").putTime(tabletTime.getMetadataTime())
          .putLocation(Location.current(tserverInstance)).putPrevEndRow(null).mutate();

      // get the tablets metadata
      var lastMetadata = testAmple.readTablet(extent);

      // update the tablets time, so it will differ
      testAmple.mutateTablet(extent).putTime(tabletTime.getMetadataTime(tabletTime.getTime() + 2))
          .mutate();

      tabletTime.updateTimeIfGreater(tabletTime.getTime() + 6);

      updateTabletDataFile(testAmple, tabletTime.getTime(), newFile, dfv1, Set.of(), 7L,
          MinorCompactionReason.SYSTEM, tserverInstance, extent, lastMetadata, tabletTime,
          flushNonce);

      var updatedMetadata = testAmple.readTablet(extent);
      assertEquals(Set.of(newFile.insert()), updatedMetadata.getFiles());
      assertEquals(tabletTime.getMetadataTime(), updatedMetadata.getTime());
      assertEquals(flushNonce, updatedMetadata.getFlushNonce().orElse(-1));

      client.tableOperations().delete(metadataTable);
    }
  }

  @Test
  public void testNoTimeUpdate() throws Exception {
    String[] tableNames = getUniqueNames(1);
    String metadataTable = tableNames[0];

    var tabletTime = TabletTime.getInstance(new MetadataTime(10, TimeType.LOGICAL));
    long flushNonce = 42L;

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(metadataTable);
      TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable));

      Ample testAmple = TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable));

      assertNull(testAmple.readTablet(extent));

      // create tablet in the fake metadata table
      testAmple.mutateTablet(extent).putDirName("dir1").putTime(tabletTime.getMetadataTime())
          .putLocation(Location.current(tserverInstance)).putPrevEndRow(null).mutate();

      // get the tablets metadata
      var lastMetadata = testAmple.readTablet(extent);

      var maxCommittedTime = tabletTime.getTime() + 2;

      // Update the tablets time, simulating concurrent bulk imports updating the time field
      // externally
      testAmple.mutateTablet(extent).putTime(tabletTime.getMetadataTime(tabletTime.getTime() + 4))
          .mutate();

      // Test the case where external bulk imports have pushed tablet time in the metadata table
      // higher than what was passed the function. In this case the time should not be updated.
      updateTabletDataFile(testAmple, maxCommittedTime, newFile, dfv1, Set.of(), 7L,
          MinorCompactionReason.SYSTEM, tserverInstance, extent, lastMetadata, tabletTime,
          flushNonce);

      var updatedMetadata = testAmple.readTablet(extent);
      assertEquals(Set.of(newFile.insert()), updatedMetadata.getFiles());
      // the time passed to updateTabletDataFile should not have been set because it was lower than
      // what is in the metadata table
      assertEquals(tabletTime.getMetadataTime(tabletTime.getTime() + 4), updatedMetadata.getTime());
      assertEquals(flushNonce, updatedMetadata.getFlushNonce().orElse(-1));

      client.tableOperations().delete(metadataTable);
    }
  }

  /**
   * This test ensures that a tablet will not add a new minor compacted file when the tablets
   * location is not as expected.
   */
  @Test
  public void testLocation() throws Exception {
    String[] tableNames = getUniqueNames(1);
    String metadataTable = tableNames[0];

    var tabletTime = TabletTime.getInstance(new MetadataTime(1, TimeType.LOGICAL));
    long flushNonce = 42L;

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(metadataTable);
      TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable));

      Ample testAmple = TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable));

      assertNull(testAmple.readTablet(extent));

      // create tablet in the fake metadata table
      testAmple.mutateTablet(extent).putDirName("dir1").putTime(tabletTime.getMetadataTime())
          .putLocation(Location.current(tserverInstance)).putPrevEndRow(null).mutate();

      // get the tablets metadata
      var lastMetadata = testAmple.readTablet(extent);

      testAmple.mutateTablet(extent).deleteLocation(Location.current(tserverInstance)).mutate();

      // try locations that differ in type, port, and session
      for (var location : List.of(Location.future(tserverInstance),
          Location.current(new TServerInstance("localhost:9998", 0xabcdef123L)),
          Location.current(new TServerInstance("localhost:9997", 0xabcdef124L)))) {
        // set a location on the tablet that will not match
        testAmple.mutateTablet(extent).putLocation(location).mutate();
        // should fail to add file to tablet because tablet location is not as expected
        assertThrows(IllegalStateException.class,
            () -> updateTabletDataFile(testAmple, tabletTime.getTime(), newFile, dfv1, Set.of(), 7L,
                MinorCompactionReason.SYSTEM, tserverInstance, extent, lastMetadata, tabletTime,
                flushNonce));
        // verify that files was not added
        var updatedMetadata = testAmple.readTablet(extent);
        assertEquals(Set.of(), updatedMetadata.getFiles());
        testAmple.mutateTablet(extent).deleteLocation(location).mutate();
      }

      client.tableOperations().delete(metadataTable);
    }
  }

  /**
   * This test exercises conditional mutations returning unknown and this causing reading of the
   * flush nonce.
   */
  @Test
  public void testFlushNonce() throws Exception {
    String[] tableNames = getUniqueNames(1);
    String metadataTable = tableNames[0];

    var tabletTime = TabletTime.getInstance(new MetadataTime(1, TimeType.LOGICAL));
    long flushNonce = 42L;

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(metadataTable);
      TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable));

      // create test ample that will return unknown status for conditional mutations.
      Ample testAmple = TestAmple.create(getCluster().getServerContext(),
          Map.of(Ample.DataLevel.USER, metadataTable), () -> withStatus(ACCEPTED, UNKNOWN, 1));

      assertNull(testAmple.readTablet(extent));

      // create tablet in the fake metadata table
      testAmple.mutateTablet(extent).putDirName("dir1").putTime(tabletTime.getMetadataTime())
          .putLocation(Location.current(tserverInstance)).putPrevEndRow(null).mutate();

      // get the tablets metadata
      var lastMetadata = testAmple.readTablet(extent);

      // This should get an unknown status and then check the flush nonce
      updateTabletDataFile(testAmple, tabletTime.getTime() + 7, newFile, dfv1, Set.of(), 7L,
          MinorCompactionReason.SYSTEM, tserverInstance, extent, lastMetadata, tabletTime,
          flushNonce);

      var updatedMetadata = testAmple.readTablet(extent);
      assertEquals(Set.of(newFile.insert()), updatedMetadata.getFiles());
      assertEquals(tabletTime.getMetadataTime(8), updatedMetadata.getTime());
      assertEquals(flushNonce, updatedMetadata.getFlushNonce().orElse(-1));

      client.tableOperations().delete(metadataTable);
    }
  }
}
