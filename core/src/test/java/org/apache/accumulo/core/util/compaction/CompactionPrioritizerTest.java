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
package org.apache.accumulo.core.util.compaction;

import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.createPriority;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.junit.jupiter.api.Test;

public class CompactionPrioritizerTest {

  public CompactionJob createJob(CompactionKind kind, String tablet, int numFiles, int totalFiles) {

    Collection<CompactableFile> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      files.add(CompactableFile
          .create(URI.create("hdfs://foonn/accumulo/tables/5/" + tablet + "/" + i + ".rf"), 4, 4));
    }
    // TODO pass numFiles
    return new CompactionJobImpl(createPriority(TableId.of("1"), kind, totalFiles, numFiles),
        CompactorGroupIdImpl.groupId("test"), files, kind, Optional.of(false));
  }

  @Test
  public void testOrdering() {
    short pr1 = createPriority(RootTable.ID, CompactionKind.USER, 10000, 1);
    assertEquals(Short.MAX_VALUE, pr1);
    short pr2 = createPriority(RootTable.ID, CompactionKind.USER, 1000, 30);
    assertTrue(pr1 > pr2);
    short pr3 = createPriority(RootTable.ID, CompactionKind.USER, 1000, 1);
    assertTrue(pr2 > pr3);
    short pr4 = createPriority(RootTable.ID, CompactionKind.USER, 1, 1);
    assertTrue(pr3 > pr4);
    short pr5 = createPriority(RootTable.ID, CompactionKind.SYSTEM, 10000, 1);
    assertTrue(pr4 > pr5);
    short pr6 = createPriority(RootTable.ID, CompactionKind.SYSTEM, 1000, 30);
    assertTrue(pr5 > pr6);
    short pr7 = createPriority(RootTable.ID, CompactionKind.SYSTEM, 1000, 1);
    assertTrue(pr6 > pr7);
    short pr8 = createPriority(RootTable.ID, CompactionKind.SYSTEM, 1, 1);
    assertTrue(pr7 > pr8);

    short pm1 = createPriority(MetadataTable.ID, CompactionKind.USER, 10000, 1);
    assertTrue(pr8 > pm1);
    short pm2 = createPriority(MetadataTable.ID, CompactionKind.USER, 1000, 30);
    assertTrue(pm1 > pm2);
    short pm3 = createPriority(MetadataTable.ID, CompactionKind.USER, 1000, 1);
    assertTrue(pm2 > pm3);
    short pm4 = createPriority(MetadataTable.ID, CompactionKind.USER, 1, 1);
    assertTrue(pm3 > pm4);
    short pm5 = createPriority(MetadataTable.ID, CompactionKind.SYSTEM, 10000, 1);
    assertTrue(pm4 > pm5);
    short pm6 = createPriority(MetadataTable.ID, CompactionKind.SYSTEM, 1000, 30);
    assertTrue(pm5 > pm6);
    short pm7 = createPriority(MetadataTable.ID, CompactionKind.SYSTEM, 1000, 1);
    assertTrue(pm6 > pm7);
    short pm8 = createPriority(MetadataTable.ID, CompactionKind.SYSTEM, 1, 1);
    assertTrue(pm7 > pm8);

    var userTable1 = TableId.of("1");
    var userTable2 = TableId.of("2");

    short pu1 = createPriority(userTable1, CompactionKind.USER, 10000, 1);
    assertTrue(pm8 > pu1);
    short pu2 = createPriority(userTable2, CompactionKind.USER, 1000, 30);
    assertTrue(pu1 > pu2);
    short pu3 = createPriority(userTable1, CompactionKind.USER, 1000, 1);
    assertTrue(pu2 > pu3);
    short pu4 = createPriority(userTable2, CompactionKind.USER, 1, 1);
    assertTrue(pu3 > pu4);
    short pu5 = createPriority(userTable1, CompactionKind.SYSTEM, 10000, 1);
    assertTrue(pu4 > pu5);
    short pu6 = createPriority(userTable2, CompactionKind.SYSTEM, 1000, 30);
    assertTrue(pu5 > pu6);
    short pu7 = createPriority(userTable1, CompactionKind.SYSTEM, 1000, 1);
    assertTrue(pu6 > pu7);
    short pu8 = createPriority(userTable2, CompactionKind.SYSTEM, 1, 1);
    assertTrue(pu7 > pu8);
    assertEquals(Short.MIN_VALUE + 2, pu8);
  }

  @Test
  public void testBoundary() {
    // test the boundary condition around the max number of files to encode
    int maxFiles = (1 << 13) - 1;
    for (var tableId : List.of(TableId.of("1"), TableId.of("2"), RootTable.ID, MetadataTable.ID)) {
      for (var kind : CompactionKind.values()) {
        short p1 = createPriority(tableId, kind, maxFiles + 10, 10);
        short p2 = createPriority(tableId, kind, maxFiles + 10, 5);
        assertEquals(p1, p2);
        short p3 = createPriority(tableId, kind, maxFiles - 2, 5);
        assertEquals(p1, p3);
        short p4 = createPriority(tableId, kind, maxFiles - 5, 5);
        assertEquals(p1, p4);
        short p5 = createPriority(tableId, kind, maxFiles - 6, 5);
        assertEquals(p1 - 1, p5);
        short p6 = createPriority(tableId, kind, maxFiles - 7, 5);
        assertEquals(p1 - 2, p6);
        short p7 = createPriority(tableId, kind, maxFiles - 17, 15);
        assertEquals(p1 - 2, p7);
        short p8 = createPriority(tableId, kind, 1, 1);
        assertEquals(p1 - maxFiles + 2, p8);
      }
    }
  }

  @Test
  public void testNegative() {
    for (var tableId : List.of(TableId.of("1"), TableId.of("2"), RootTable.ID, MetadataTable.ID)) {
      for (var kind : CompactionKind.values()) {
        assertThrows(IllegalArgumentException.class, () -> createPriority(tableId, kind, -5, 2));
        assertThrows(IllegalArgumentException.class, () -> createPriority(tableId, kind, 10, -5));
      }
    }
  }

  @Test
  public void testCompactionJobComparator() {
    var j1 = createJob(CompactionKind.USER, "t-009", 10, 20);
    var j2 = createJob(CompactionKind.USER, "t-010", 11, 25);
    var j3 = createJob(CompactionKind.USER, "t-011", 11, 20);
    var j4 = createJob(CompactionKind.SYSTEM, "t-012", 11, 30);
    var j5 = createJob(CompactionKind.SYSTEM, "t-013", 5, 10);
    var j8 = createJob(CompactionKind.SELECTOR, "t-014", 5, 21);
    var j9 = createJob(CompactionKind.SELECTOR, "t-015", 7, 20);

    var expected = List.of(j2, j3, j1, j4, j9, j8, j5);

    var shuffled = new ArrayList<>(expected);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled, CompactionJobPrioritizer.JOB_COMPARATOR);

    assertEquals(expected, shuffled);
  }
}
