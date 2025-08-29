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
package org.apache.accumulo.test.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AbstractTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FileMetadataUtil {

  private static final Logger log = LoggerFactory.getLogger(FileMetadataUtil.class);

  public static Map<StoredTabletFile,DataFileValue>
      printAndVerifyFileMetadata(final ServerContext ctx, TableId tableId) {
    return printAndVerifyFileMetadata(ctx, tableId, -1);
  }

  public static Map<StoredTabletFile,DataFileValue>
      printAndVerifyFileMetadata(final ServerContext ctx, TableId tableId, int expectedFiles) {
    final Map<StoredTabletFile,DataFileValue> files = new HashMap<>();

    try (TabletsMetadata tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId)
        .fetch(ColumnType.FILES, ColumnType.PREV_ROW).build()) {

      // Read each file referenced by that table
      int i = 0;
      for (TabletMetadata tabletMetadata : tabletsMetadata) {
        for (Entry<StoredTabletFile,
            DataFileValue> fileEntry : new TreeMap<>(tabletMetadata.getFilesMap()).entrySet()) {
          StoredTabletFile file = fileEntry.getKey();
          DataFileValue dfv = fileEntry.getValue();
          files.put(file, dfv);
          log.debug("Extent: " + tabletMetadata.getExtent() + "; File Name: " + file.getFileName()
              + "; Range: " + file.getRange() + "; Entries: " + dfv.getNumEntries() + ", Size: "
              + dfv.getSize());
          i++;
        }
      }

      log.debug("");
      if (expectedFiles >= 0) {
        assertEquals(expectedFiles, i);
      }
    }

    return files;
  }

  public static int countFiles(final ServerContext ctx, String tableName) {
    return countFiles(ctx, tableName, null, null);
  }

  public static int countFiles(final ServerContext ctx, String tableName, Text tabletStartRow,
      Text tabletEndRow) {
    final TableId tableId = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));
    try (TabletsMetadata tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId)
        .overlapping(tabletStartRow, tabletEndRow).fetch(ColumnType.FILES).build()) {
      return tabletsMetadata.stream().mapToInt(tm -> tm.getFilesMap().size()).sum();
    }
  }

  public static int countFencedFiles(final ServerContext ctx, String tableName) {
    return countFencedFiles(ctx, tableName, null, null);
  }

  public static int countFencedFiles(final ServerContext ctx, String tableName, Text tabletStartRow,
      Text tabletEndRow) {
    final TableId tableId = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));
    try (TabletsMetadata tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId)
        .overlapping(tabletStartRow, tabletEndRow).fetch(ColumnType.FILES).build()) {
      return (int) tabletsMetadata.stream().flatMap(tm -> tm.getFilesMap().keySet().stream())
          .filter(AbstractTabletFile::hasRange).count();
    }
  }

  public static void splitFilesIntoRanges(final ServerContext ctx, String tableName,
      Set<Range> fileRanges) throws Exception {
    splitFilesIntoRanges(ctx, tableName, null, null, fileRanges);
  }

  public static void splitFilesIntoRanges(final ServerContext ctx, String tableName,
      Text tabletStartRow, Text tabletEndRow, Set<Range> fileRanges) throws Exception {

    Preconditions.checkArgument(!fileRanges.isEmpty(), "Ranges must not be empty");

    mutateTabletFiles(ctx, tableName, tabletStartRow, tabletEndRow, (tm, mutator, file, value) -> {
      // Create a mutation to delete the existing file metadata entry with infinite range
      mutator.deleteFile(file);

      fileRanges.forEach(range -> {
        final DataFileValue newValue =
            new DataFileValue(Integer.max(1, (int) (value.getSize() / fileRanges.size())),
                Integer.max(1, (int) (value.getNumEntries() / fileRanges.size())));
        mutator.putFile(StoredTabletFile.of(file.getPath(), range), newValue);
      });
    });
  }

  public static void mutateTabletFiles(final ServerContext ctx, String tableName,
      Text tabletStartRow, Text tabletEndRow, FileMutator fileMutator) throws Exception {

    final TableId tableId = TableId.of(ctx.tableOperations().tableIdMap().get(tableName));

    // Bring tablet offline so we can modify file metadata
    ctx.tableOperations().offline(tableName, true);

    try (TabletsMetadata tabletsMetadata =
        ctx.getAmple().readTablets().forTable(tableId).overlapping(tabletStartRow, tabletEndRow)
            .fetch(ColumnType.FILES, ColumnType.PREV_ROW).build()) {

      // Process each tablet in the given start/end row range
      for (TabletMetadata tabletMetadata : tabletsMetadata) {
        final KeyExtent ke = tabletMetadata.getExtent();

        TabletMutator mutator = ctx.getAmple().mutateTablet(ke);

        // Read each file and mutate
        for (Entry<StoredTabletFile,DataFileValue> fileEntry : tabletMetadata.getFilesMap()
            .entrySet()) {
          StoredTabletFile file = fileEntry.getKey();
          DataFileValue value = fileEntry.getValue();

          // do any file mutations
          fileMutator.mutate(tabletMetadata, mutator, file, value);

          mutator.mutate();
        }
      }
    }

    // Bring back online after metadata updates
    ctx.tableOperations().online(tableName, true);
  }

  // Verifies that the MERGED marker was cleared and doesn't exist on any tablet
  public static void verifyMergedMarkerCleared(final ServerContext ctx, TableId tableId) {
    try (var tabletsMetadata =
        ctx.getAmple().readTablets().forTable(tableId).fetch(ColumnType.MERGED).build()) {
      assertTrue(tabletsMetadata.stream().noneMatch(TabletMetadata::hasMerged));
    }
  }

  public interface FileMutator {
    void mutate(TabletMetadata tm, TabletMutator mutator, StoredTabletFile file,
        DataFileValue value);
  }
}
