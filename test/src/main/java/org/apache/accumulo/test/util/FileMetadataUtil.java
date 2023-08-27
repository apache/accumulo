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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        for (Entry<StoredTabletFile,DataFileValue> fileEntry : tabletMetadata.getFilesMap()
            .entrySet()) {
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
}
