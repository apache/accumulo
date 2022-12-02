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
package org.apache.accumulo.core.metadata.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.schema.Section;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Describes the table schema used for metadata tables
 */
public class MetadataSchema {

  public static final String RESERVED_PREFIX = "~";

  /**
   * Used for storing information about tablets
   */
  public static class TabletsSection {
    private static final Section section = new Section(null, false, RESERVED_PREFIX, false);

    public static Range getRange() {
      return section.getRange();
    }

    public static Range getRange(TableId tableId) {
      return new Range(new Key(tableId.canonical() + ';'), true,
          new Key(tableId.canonical() + '<').followingKey(PartialKey.ROW), false);
    }

    public static Text encodeRow(TableId tableId, Text endRow) {
      Text entry = new Text(tableId.canonical());

      if (endRow == null) {
        // append delimiter for default tablet
        entry.append(new byte[] {'<'}, 0, 1);
      } else {
        // append delimiter for regular tablets
        entry.append(new byte[] {';'}, 0, 1);
        entry.append(endRow.getBytes(), 0, endRow.getLength());
      }

      return entry;
    }

    /**
     * Decodes a metadata row into a pair of table ID and end row.
     */
    public static Pair<TableId,Text> decodeRow(Text metadataRow) {
      int semiPos = -1;
      int ltPos = -1;

      for (int i = 0; i < metadataRow.getLength(); i++) {
        if (metadataRow.getBytes()[i] == ';' && semiPos < 0) {
          // want the position of the first semicolon
          semiPos = i;
        }
        if (metadataRow.getBytes()[i] == '<') {
          ltPos = i;
        }
      }

      if (semiPos < 0 && ltPos < 0) {
        throw new IllegalArgumentException("Metadata row does not contain ; or <  " + metadataRow);
      }

      if (semiPos < 0) {
        // default tablet ending in '<'
        if (ltPos != metadataRow.getLength() - 1) {
          throw new IllegalArgumentException("< must come at end of Metadata row  " + metadataRow);
        }
        TableId tableId = TableId.of(new String(metadataRow.getBytes(), 0, ltPos, UTF_8));
        return new Pair<>(tableId, null);
      } else {
        // other tablets containing ';'
        TableId tableId = TableId.of(new String(metadataRow.getBytes(), 0, semiPos, UTF_8));
        Text endRow = new Text();
        endRow.set(metadataRow.getBytes(), semiPos + 1, metadataRow.getLength() - (semiPos + 1));
        return new Pair<>(tableId, endRow);
      }
    }

    /**
     * Column family for storing the tablet information needed by clients
     */
    public static class TabletColumnFamily {
      /**
       * This needs to sort after all other column families for that tablet, because the
       * {@link #PREV_ROW_COLUMN} sits in this and that needs to sort last because the
       * SimpleGarbageCollector relies on this.
       */
      public static final String STR_NAME = "~tab";
      public static final Text NAME = new Text(STR_NAME);
      /**
       * README : very important that prevRow sort last to avoid race conditions between garbage
       * collector and split this needs to sort after everything else for that tablet
       */
      public static final String PREV_ROW_QUAL = "~pr";
      public static final ColumnFQ PREV_ROW_COLUMN = new ColumnFQ(NAME, new Text(PREV_ROW_QUAL));

      public static Value encodePrevEndRow(Text per) {
        if (per == null) {
          return new Value(new byte[] {0});
        }
        byte[] b = new byte[per.getLength() + 1];
        b[0] = 1;
        System.arraycopy(per.getBytes(), 0, b, 1, per.getLength());
        return new Value(b);
      }

      public static Text decodePrevEndRow(Value ibw) {
        Text per = null;

        if (ibw.get()[0] != 0) {
          per = new Text();
          per.set(ibw.get(), 1, ibw.get().length - 1);
        }

        return per;
      }

      /**
       * A temporary field in case a split fails and we need to roll back
       */
      public static final String OLD_PREV_ROW_QUAL = "oldprevrow";
      public static final ColumnFQ OLD_PREV_ROW_COLUMN =
          new ColumnFQ(NAME, new Text(OLD_PREV_ROW_QUAL));
      /**
       * A temporary field for splits to optimize certain operations
       */
      public static final String SPLIT_RATIO_QUAL = "splitRatio";
      public static final ColumnFQ SPLIT_RATIO_COLUMN =
          new ColumnFQ(NAME, new Text(SPLIT_RATIO_QUAL));

      /**
       * Creates a mutation that encodes a KeyExtent as a prevRow entry.
       */
      public static Mutation createPrevRowMutation(KeyExtent ke) {
        Mutation m = new Mutation(ke.toMetaRow());
        PREV_ROW_COLUMN.put(m, encodePrevEndRow(ke.prevEndRow()));
        return m;
      }
    }

    /**
     * Column family for recording information used by the TServer
     */
    public static class ServerColumnFamily {
      public static final String STR_NAME = "srv";
      public static final Text NAME = new Text(STR_NAME);
      /**
       * Holds the location of the tablet in the DFS file system
       */
      public static final String DIRECTORY_QUAL = "dir";
      public static final ColumnFQ DIRECTORY_COLUMN = new ColumnFQ(NAME, new Text(DIRECTORY_QUAL));
      /**
       * Initial tablet directory name for the default tablet in all tables
       */
      public static final String DEFAULT_TABLET_DIR_NAME = "default_tablet";

      /**
       * Matches regex for a tablet directory like "default_tablet" or "t-000009x"
       *
       * @return true if dirName is a valid value for the {@link #DIRECTORY_COLUMN} in the metadata
       *         table. Returns false otherwise.
       */
      public static boolean isValidDirCol(String dirName) {
        return dirName.matches("[\\dA-Za-z_-]+");
      }

      /**
       * @throws IllegalArgumentException when {@link #isValidDirCol(String)} returns false.
       */
      public static void validateDirCol(String dirName) {
        Preconditions.checkArgument(isValidDirCol(dirName), "Invalid dir name %s", dirName);
      }

      /**
       * Holds the {@link TimeType}
       */
      public static final String TIME_QUAL = "time";
      public static final ColumnFQ TIME_COLUMN = new ColumnFQ(NAME, new Text(TIME_QUAL));
      /**
       * Holds flush IDs to enable waiting on a flush to complete
       */
      public static final String FLUSH_QUAL = "flush";
      public static final ColumnFQ FLUSH_COLUMN = new ColumnFQ(NAME, new Text(FLUSH_QUAL));
      /**
       * Holds compact IDs to enable waiting on a compaction to complete
       */
      public static final String COMPACT_QUAL = "compact";
      public static final ColumnFQ COMPACT_COLUMN = new ColumnFQ(NAME, new Text(COMPACT_QUAL));
      /**
       * Holds lock IDs to enable a sanity check to ensure that the TServer writing to the metadata
       * tablet is not dead
       */
      public static final String LOCK_QUAL = "lock";
      public static final ColumnFQ LOCK_COLUMN = new ColumnFQ(NAME, new Text(LOCK_QUAL));
    }

    /**
     * Column family for storing entries created by the TServer to indicate it has loaded a tablet
     * that it was assigned
     */
    public static class CurrentLocationColumnFamily {
      public static final String STR_NAME = "loc";
      public static final Text NAME = new Text(STR_NAME);
    }

    /**
     * Column family for storing the assigned location
     */
    public static class FutureLocationColumnFamily {
      public static final String STR_NAME = "future";
      public static final Text NAME = new Text(STR_NAME);
    }

    /**
     * Column family for storing last location, as a hint for assignment
     */
    public static class LastLocationColumnFamily {
      public static final String STR_NAME = "last";
      public static final Text NAME = new Text(STR_NAME);
    }

    /**
     * Column family for storing suspension location, as a demand for assignment.
     */
    public static class SuspendLocationColumn {
      public static final String STR_NAME = "suspend";
      public static final ColumnFQ SUSPEND_COLUMN =
          new ColumnFQ(new Text(STR_NAME), new Text("loc"));
    }

    /**
     * Temporary markers that indicate a tablet loaded a bulk file
     */
    public static class BulkFileColumnFamily {
      public static final String STR_NAME = "loaded";
      public static final Text NAME = new Text(STR_NAME);

      public static long getBulkLoadTid(Value v) {
        return getBulkLoadTid(v.toString());
      }

      public static long getBulkLoadTid(String vs) {
        if (FateTxId.isFormatedTid(vs)) {
          return FateTxId.fromString(vs);
        } else {
          // a new serialization format was introduce in 2.0. This code support deserializing the
          // old format.
          return Long.parseLong(vs);
        }
      }
    }

    /**
     * Temporary marker that indicates a tablet was successfully cloned
     */
    public static class ClonedColumnFamily {
      public static final String STR_NAME = "!cloned";
      public static final Text NAME = new Text(STR_NAME);
    }

    /**
     * Column family for storing files used by a tablet
     */
    public static class DataFileColumnFamily {
      public static final String STR_NAME = "file";
      public static final Text NAME = new Text(STR_NAME);
    }

    /**
     * Column family for storing the set of files scanned with an isolated scanner, to prevent them
     * from being deleted
     */
    public static class ScanFileColumnFamily {
      public static final String STR_NAME = "scan";
      public static final Text NAME = new Text(STR_NAME);
    }

    /**
     * Column family for storing write-ahead log entries
     */
    public static class LogColumnFamily {
      public static final String STR_NAME = "log";
      public static final Text NAME = new Text(STR_NAME);
    }

    /**
     * Column family for indicating that the files in a tablet have been trimmed to only include
     * data for the current tablet, so that they are safe to merge
     */
    public static class ChoppedColumnFamily {
      public static final String STR_NAME = "chopped";
      public static final Text NAME = new Text(STR_NAME);
      public static final ColumnFQ CHOPPED_COLUMN = new ColumnFQ(NAME, new Text(STR_NAME));
    }

    public static class ExternalCompactionColumnFamily {
      public static final String STR_NAME = "ecomp";
      public static final Text NAME = new Text(STR_NAME);
    }
  }

  /**
   * Contains additional metadata in a reserved area not for tablets
   */
  public static class ReservedSection {
    private static final Section section = new Section(RESERVED_PREFIX, true, null, false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }

  }

  /**
   * Holds delete markers for potentially unused files/directories
   */
  public static class DeletesSection {
    private static final Section section =
        new Section(RESERVED_PREFIX + "del", true, RESERVED_PREFIX + "dem", false);

    private static final int encoded_prefix_length =
        section.getRowPrefix().length() + SortSkew.SORTSKEW_LENGTH;

    public static Range getRange() {
      return section.getRange();
    }

    public static String encodeRow(String value) {
      return section.getRowPrefix() + SortSkew.getCode(value) + value;
    }

    public static String decodeRow(String row) {
      return row.substring(encoded_prefix_length);
    }

    /**
     * Value to indicate that the row has been skewed/encoded.
     */
    public static class SkewedKeyValue {
      public static final String STR_NAME = "skewed";
      public static final Value NAME = new Value(STR_NAME);
    }

  }

  /**
   * Holds bulk-load-in-progress processing flags
   */
  public static class BlipSection {
    private static final Section section =
        new Section(RESERVED_PREFIX + "blip", true, RESERVED_PREFIX + "bliq", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }

  }

  public static class ExternalCompactionSection {
    private static final Section section =
        new Section(RESERVED_PREFIX + "ecomp", true, RESERVED_PREFIX + "ecomq", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }
  }

  public static class ScanServerFileReferenceSection {
    private static final Section section =
        new Section(RESERVED_PREFIX + "sserv", true, RESERVED_PREFIX + "sserx", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }
  }
}
