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

import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.METADATA_TABLE_SYSTEM;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.METADATA_TABLE_USER;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.ROOT_TABLE_SYSTEM;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.ROOT_TABLE_USER;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.SYSTEM_NS_SYSTEM;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.SYSTEM_NS_USER;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.TABLE_OVER_SIZE;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.USER_TABLE_SYSTEM;
import static org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer.USER_TABLE_USER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.junit.jupiter.api.Test;

public class CompactionPrioritizerTest {

  private static final int TABLET_FILE_MAX = 3001;

  public CompactionJob createJob(CompactionKind kind, String tablet, int numFiles, int totalFiles) {

    Collection<CompactableFile> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      files.add(CompactableFile
          .create(URI.create("hdfs://foonn/accumulo/tables/5/" + tablet + "/" + i + ".rf"), 4, 4));
    }
    return new CompactionJobImpl(
        CompactionJobPrioritizer.createPriority(Namespace.DEFAULT.id(), TableId.of("5"), kind,
            totalFiles, numFiles, totalFiles * 2),
        CompactionExecutorIdImpl.externalId("test"), files, kind, Optional.of(false));
  }

  @Test
  public void testRootTablePriorities() {
    assertEquals(ROOT_TABLE_USER.getMinimum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.USER, 0, 0, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_USER.getMinimum() + 1000, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.USER, 1000, 0, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_USER.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.USER, 3000, 0, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_SYSTEM.getMinimum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.SYSTEM, 0, 0, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_SYSTEM.getMinimum() + 1000, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.SYSTEM, 1000, 0, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_SYSTEM.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.SYSTEM, 3000, 0, TABLET_FILE_MAX));
  }

  @Test
  public void testMetaTablePriorities() {
    assertEquals(METADATA_TABLE_USER.getMinimum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), MetadataTable.ID, CompactionKind.USER, 0, 0, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_USER.getMinimum() + 1000, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), MetadataTable.ID, CompactionKind.USER, 1000, 0, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_USER.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), MetadataTable.ID, CompactionKind.USER, 3000, 0, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_SYSTEM.getMinimum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), MetadataTable.ID, CompactionKind.SYSTEM, 0, 0, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_SYSTEM.getMinimum() + 1000,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(), MetadataTable.ID,
            CompactionKind.SYSTEM, 1000, 0, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_SYSTEM.getMaximum(),
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(), MetadataTable.ID,
            CompactionKind.SYSTEM, 3000, 0, TABLET_FILE_MAX));
  }

  @Test
  public void testSystemNamespacePriorities() {
    TableId tid = TableId.of("someOtherSystemTable");
    assertEquals(SYSTEM_NS_USER.getMinimum(), CompactionJobPrioritizer
        .createPriority(Namespace.ACCUMULO.id(), tid, CompactionKind.USER, 0, 0, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_USER.getMinimum() + 1000, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.USER, 1000, 0, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_USER.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.USER, 3000, 0, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_SYSTEM.getMinimum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 0, 0, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_SYSTEM.getMinimum() + 1000, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 1000, 0, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_SYSTEM.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 3000, 0, TABLET_FILE_MAX));
  }

  @Test
  public void testUserTablePriorities() {
    TableId tid = TableId.of("someUserTable");
    assertEquals(USER_TABLE_USER.getMinimum(), CompactionJobPrioritizer
        .createPriority(Namespace.DEFAULT.id(), tid, CompactionKind.USER, 0, 0, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_USER.getMinimum() + 1000, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.USER, 1000, 0, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_USER.getMinimum() + 3000, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.USER, 3000, 0, TABLET_FILE_MAX));

    assertEquals(USER_TABLE_SYSTEM.getMinimum(), CompactionJobPrioritizer
        .createPriority(Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 0, 0, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_SYSTEM.getMinimum() + 1000, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 1000, 0, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_SYSTEM.getMinimum() + 3000, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 3000, 0, TABLET_FILE_MAX));
  }

  @Test
  public void testTableOverSize() {
    final int tabletFileMax = 30;
    final TableId tid = TableId.of("someTable");
    assertEquals(ROOT_TABLE_SYSTEM.getMinimum() + 150, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(METADATA_TABLE_SYSTEM.getMinimum() + 150, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), MetadataTable.ID, CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(SYSTEM_NS_SYSTEM.getMinimum() + 150, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(TABLE_OVER_SIZE.getMinimum() + 120, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(ROOT_TABLE_SYSTEM.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), RootTable.ID, CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
    assertEquals(METADATA_TABLE_SYSTEM.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), MetadataTable.ID, CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
    assertEquals(SYSTEM_NS_SYSTEM.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
    assertEquals(TABLE_OVER_SIZE.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
  }

  @Test
  public void testCompactionJobComparator() {
    var j1 = createJob(CompactionKind.USER, "t-009", 10, 20); // 30
    var j2 = createJob(CompactionKind.USER, "t-010", 11, 25); // 36
    var j3 = createJob(CompactionKind.USER, "t-011", 11, 20); // 31
    var j4 = createJob(CompactionKind.SYSTEM, "t-012", 11, 30); // 40
    var j5 = createJob(CompactionKind.SYSTEM, "t-013", 5, 10); // 15
    var j6 = createJob(CompactionKind.CHOP, "t-014", 5, 40); // 45
    var j7 = createJob(CompactionKind.CHOP, "t-015", 5, 7); // 12
    var j8 = createJob(CompactionKind.SELECTOR, "t-014", 5, 21); // 26
    var j9 = createJob(CompactionKind.SELECTOR, "t-015", 7, 20); // 27

    var expected = List.of(j6, j2, j3, j1, j7, j4, j9, j8, j5);

    var shuffled = new ArrayList<>(expected);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled, CompactionJobPrioritizer.JOB_COMPARATOR);

    assertEquals(expected, shuffled);
  }
}
