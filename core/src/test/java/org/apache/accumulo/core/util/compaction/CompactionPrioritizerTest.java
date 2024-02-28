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
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
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
        CompactorGroupId.of("test"), files, kind, Optional.of(false));
  }

  @Test
  public void testOrdering() {
    short pr1 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.USER, 10000, 1);
    assertEquals(Short.MAX_VALUE, pr1);
    short pr2 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.USER, 100, 30);
    assertTrue(pr1 > pr2);
    short pr3 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.USER, 100, 1);
    assertTrue(pr2 > pr3);
    short pr4 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.USER, 1, 1);
    assertTrue(pr3 > pr4);
    short pr5 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.SYSTEM, 10000, 1);
    assertTrue(pr4 > pr5);
    short pr6 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.SYSTEM, 100, 30);
    assertTrue(pr5 > pr6);
    short pr7 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.SYSTEM, 100, 1);
    assertTrue(pr6 > pr7);
    short pr8 = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.SYSTEM, 1, 1);
    assertTrue(pr7 > pr8);

    short pm1 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.USER, 10000, 1);
    assertTrue(pr8 > pm1);
    short pm2 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.USER, 100, 30);
    assertTrue(pm1 > pm2);
    short pm3 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.USER, 100, 1);
    assertTrue(pm2 > pm3);
    short pm4 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.USER, 1, 1);
    assertTrue(pm3 > pm4);
    short pm5 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.SYSTEM, 10000, 1);
    assertTrue(pm4 > pm5);
    short pm6 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.SYSTEM, 100, 30);
    assertTrue(pm5 > pm6);
    short pm7 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.SYSTEM, 100, 1);
    assertTrue(pm6 > pm7);
    short pm8 = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.SYSTEM, 1, 1);
    assertTrue(pm7 > pm8);

    short pf1 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.USER, 10000, 1);
    assertTrue(pm8 > pf1);
    short pf2 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.USER, 100, 30);
    assertTrue(pf1 > pf2);
    short pf3 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.USER, 100, 1);
    assertTrue(pf2 > pf3);
    short pf4 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.USER, 1, 1);
    assertTrue(pf3 > pf4);
    short pf5 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.SYSTEM, 10000, 1);
    assertTrue(pf4 > pf5);
    short pf6 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.SYSTEM, 100, 30);
    assertTrue(pf5 > pf6);
    short pf7 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.SYSTEM, 100, 1);
    assertTrue(pf6 > pf7);
    short pf8 = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.SYSTEM, 1, 1);
    assertTrue(pm7 > pf8);

    var userTable1 = TableId.of("1");
    var userTable2 = TableId.of("2");

    short pu1 = createPriority(userTable1, CompactionKind.USER, 10000, 1);
    assertTrue(pf8 > pu1);
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
    var userTable = TableId.of("1");

    short minRootUser = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.USER, 1, 1);
    short minRootSystem = createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.SYSTEM, 1, 1);
    short minMetaUser = createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.USER, 1, 1);
    short minMetaSystem =
        createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.SYSTEM, 1, 1);
    short minFateUser = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.USER, 1, 1);
    short minFateSystem = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.SYSTEM, 1, 1);
    short minUserUser = createPriority(userTable, CompactionKind.USER, 1, 1);

    // Test the boundary condition around the max number of files to encode. Ensure the next level
    // is always greater no matter how many files.
    for (int files = 1; files < 100_000; files += 1) {
      short rootSystem =
          createPriority(AccumuloTable.ROOT.tableId(), CompactionKind.SYSTEM, files, 1);
      assertTrue(minRootUser > rootSystem);
      short metaUser =
          createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.USER, files, 1);
      assertTrue(minRootSystem > metaUser);
      short metaSystem =
          createPriority(AccumuloTable.METADATA.tableId(), CompactionKind.SYSTEM, files, 1);
      assertTrue(minMetaUser > metaSystem);
      short fateUser = createPriority(AccumuloTable.FATE.tableId(), CompactionKind.USER, files, 1);
      assertTrue(minMetaSystem > fateUser);
      short fateSystem =
          createPriority(AccumuloTable.FATE.tableId(), CompactionKind.SYSTEM, files, 1);
      assertTrue(minFateUser > fateSystem);
      short userUser = createPriority(userTable, CompactionKind.USER, files, 1);
      assertTrue(minFateSystem > userUser);
      short userSystem = createPriority(userTable, CompactionKind.SYSTEM, files, 1);
      assertTrue(minUserUser > userSystem);
    }

  }

  @Test
  public void testNegative() {
    for (var tableId : List.of(TableId.of("1"), TableId.of("2"), AccumuloTable.ROOT.tableId(),
        AccumuloTable.METADATA.tableId())) {
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
