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
