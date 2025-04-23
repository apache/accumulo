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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloNamespace;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.commons.lang3.Range;
import org.junit.jupiter.api.Test;

public class CompactionPrioritizerTest {

  private static final int TABLET_FILE_MAX = 3001;

  public CompactionJob createJob(CompactionKind kind, String tablet, int numFiles, int totalFiles) {

    Collection<CompactableFile> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      files.add(CompactableFile
          .create(URI.create("hdfs://foonn/accumulo/tables/5/" + tablet + "/" + i + ".rf"), 4, 4));
    }
    return new CompactionJobImpl(CompactionJobPrioritizer.createPriority(Namespace.DEFAULT.id(),
        TableId.of("5"), kind, totalFiles, numFiles, totalFiles * 2), CompactorGroupId.of("test"),
        files, kind);
  }

  @Test
  public void testNonOverlappingRanges() {
    List<Range<Short>> ranges = new ArrayList<>();
    ranges.add(ROOT_TABLE_USER);
    ranges.add(ROOT_TABLE_SYSTEM);
    ranges.add(METADATA_TABLE_USER);
    ranges.add(METADATA_TABLE_SYSTEM);
    ranges.add(SYSTEM_NS_USER);
    ranges.add(SYSTEM_NS_SYSTEM);
    ranges.add(TABLE_OVER_SIZE);
    ranges.add(USER_TABLE_USER);
    ranges.add(USER_TABLE_SYSTEM);

    for (Range<Short> r1 : ranges) {
      for (Range<Short> r2 : ranges) {
        if (r1 == r2) {
          continue;
        }
        assertFalse(r1.isOverlappedBy(r2), r1.toString() + " is overlapped by " + r2.toString());
      }
    }

    Collections.sort(ranges, new Comparator<Range<Short>>() {
      @Override
      public int compare(Range<Short> r1, Range<Short> r2) {
        return Short.compare(r1.getMinimum(), r2.getMinimum());
      }
    });
    assertEquals(Short.MIN_VALUE, ranges.get(0).getMinimum());
    assertEquals(Short.MAX_VALUE, ranges.get(ranges.size() - 1).getMaximum());
    // check that the max of the previous range is one less than the
    // minimum of the current range to make sure there are no holes.
    short lastMax = Short.MIN_VALUE;
    for (Range<Short> r : ranges) {
      if (lastMax != Short.MIN_VALUE) {
        assertTrue(r.getMinimum() - lastMax == 1);
      }
      lastMax = r.getMaximum();
    }
  }

  @Test
  public void testRootTablePriorities() {
    assertEquals(ROOT_TABLE_USER.getMinimum() + 1,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.USER, 0, 1, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_USER.getMinimum() + 1010,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.USER, 1000, 10, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_USER.getMaximum(),
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.USER, 3000, 100, TABLET_FILE_MAX));

    assertEquals(ROOT_TABLE_SYSTEM.getMinimum() + 3,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.SYSTEM, 0, 3, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_SYSTEM.getMinimum() + 1030,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.SYSTEM, 1000, 30, TABLET_FILE_MAX));
    assertEquals(ROOT_TABLE_SYSTEM.getMaximum(),
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.SYSTEM, 3000, 300, TABLET_FILE_MAX));
  }

  @Test
  public void testMetaTablePriorities() {
    assertEquals(METADATA_TABLE_USER.getMinimum() + 4,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.USER, 0, 4, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_USER.getMinimum() + 1040,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.USER, 1000, 40, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_USER.getMaximum(),
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.USER, 3000, 400, TABLET_FILE_MAX));

    assertEquals(METADATA_TABLE_SYSTEM.getMinimum() + 6,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.SYSTEM, 0, 6, TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_SYSTEM.getMinimum() + 1060,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.SYSTEM, 1000, 60,
            TABLET_FILE_MAX));
    assertEquals(METADATA_TABLE_SYSTEM.getMaximum(),
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.SYSTEM, 3000, 600,
            TABLET_FILE_MAX));
  }

  @Test
  public void testSystemNamespacePriorities() {
    TableId tid = TableId.of("someOtherSystemTable");
    assertEquals(SYSTEM_NS_USER.getMinimum() + 7, CompactionJobPrioritizer
        .createPriority(Namespace.ACCUMULO.id(), tid, CompactionKind.USER, 0, 7, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_USER.getMinimum() + 1070, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.USER, 1000, 70, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_USER.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.USER, 3000, 700, TABLET_FILE_MAX));

    assertEquals(SYSTEM_NS_SYSTEM.getMinimum() + 9, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 0, 9, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_SYSTEM.getMinimum() + 1090, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 1000, 90, TABLET_FILE_MAX));
    assertEquals(SYSTEM_NS_SYSTEM.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 3000, 900, TABLET_FILE_MAX));
  }

  @Test
  public void testUserTablePriorities() {
    TableId tid = TableId.of("someUserTable");
    assertEquals(USER_TABLE_USER.getMinimum() + 10, CompactionJobPrioritizer
        .createPriority(Namespace.DEFAULT.id(), tid, CompactionKind.USER, 0, 10, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_USER.getMinimum() + 1100, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.USER, 1000, 100, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_USER.getMinimum() + 4000, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.USER, 3000, 1000, TABLET_FILE_MAX));

    assertEquals(USER_TABLE_SYSTEM.getMinimum() + 11, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 0, 11, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_SYSTEM.getMinimum() + 1110, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 1000, 110, TABLET_FILE_MAX));
    assertEquals(USER_TABLE_SYSTEM.getMinimum() + 4100, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 3000, 1100, TABLET_FILE_MAX));
  }

  @Test
  public void testTableOverSize() {
    final int tabletFileMax = 30;
    final TableId tid = TableId.of("someTable");
    assertEquals(ROOT_TABLE_SYSTEM.getMinimum() + 150,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(METADATA_TABLE_SYSTEM.getMinimum() + 150,
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(SYSTEM_NS_SYSTEM.getMinimum() + 150, CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(TABLE_OVER_SIZE.getMinimum() + 120, CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 100, 50, tabletFileMax));
    assertEquals(ROOT_TABLE_SYSTEM.getMaximum(),
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.ROOT.tableId(), CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
    assertEquals(METADATA_TABLE_SYSTEM.getMaximum(),
        CompactionJobPrioritizer.createPriority(Namespace.ACCUMULO.id(),
            AccumuloNamespace.METADATA.tableId(), CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
    assertEquals(SYSTEM_NS_SYSTEM.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.ACCUMULO.id(), tid, CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
    assertEquals(TABLE_OVER_SIZE.getMaximum(), CompactionJobPrioritizer.createPriority(
        Namespace.DEFAULT.id(), tid, CompactionKind.SYSTEM, 3000, 50, tabletFileMax));
  }

  @Test
  public void testCompactionJobComparator() {
    var j1 = createJob(CompactionKind.USER, "t-009", 10, 20);
    var j2 = createJob(CompactionKind.USER, "t-010", 11, 25);
    var j3 = createJob(CompactionKind.USER, "t-011", 11, 20);
    var j4 = createJob(CompactionKind.SYSTEM, "t-012", 11, 30);
    var j5 = createJob(CompactionKind.SYSTEM, "t-013", 5, 10);
    var j8 = createJob(CompactionKind.SYSTEM, "t-014", 5, 21);
    var j9 = createJob(CompactionKind.SYSTEM, "t-015", 7, 20);

    var expected = List.of(j2, j3, j1, j4, j9, j8, j5);

    var shuffled = new ArrayList<>(expected);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled, CompactionJobPrioritizer.JOB_COMPARATOR);

    assertEquals(expected, shuffled);
  }
}
