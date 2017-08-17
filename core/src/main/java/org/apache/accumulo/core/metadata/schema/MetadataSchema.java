/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.metadata.schema;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.schema.Section;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;

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

    public static Range getRange(Table.ID tableId) {
      return new Range(new Key(tableId.canonicalID() + ';'), true, new Key(tableId.canonicalID() + '<').followingKey(PartialKey.ROW), false);
    }

    public static Text getRow(Table.ID tableId, Text endRow) {
      Text entry = new Text(tableId.getUtf8());

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
     * Column family for storing the tablet information needed by clients
     */
    public static class TabletColumnFamily {
      /**
       * This needs to sort after all other column families for that tablet, because the {@link #PREV_ROW_COLUMN} sits in this and that needs to sort last
       * because the SimpleGarbageCollector relies on this.
       */
      public static final Text NAME = new Text("~tab");
      /**
       * README : very important that prevRow sort last to avoid race conditions between garbage collector and split this needs to sort after everything else
       * for that tablet
       */
      public static final ColumnFQ PREV_ROW_COLUMN = new ColumnFQ(NAME, new Text("~pr"));
      /**
       * A temporary field in case a split fails and we need to roll back
       */
      public static final ColumnFQ OLD_PREV_ROW_COLUMN = new ColumnFQ(NAME, new Text("oldprevrow"));
      /**
       * A temporary field for splits to optimize certain operations
       */
      public static final ColumnFQ SPLIT_RATIO_COLUMN = new ColumnFQ(NAME, new Text("splitRatio"));
    }

    /**
     * Column family for recording information used by the TServer
     */
    public static class ServerColumnFamily {
      public static final Text NAME = new Text("srv");
      /**
       * Holds the location of the tablet in the DFS file system
       */
      public static final ColumnFQ DIRECTORY_COLUMN = new ColumnFQ(NAME, new Text("dir"));
      /**
       * Holds the {@link TimeType}
       */
      public static final ColumnFQ TIME_COLUMN = new ColumnFQ(NAME, new Text("time"));
      /**
       * Holds flush IDs to enable waiting on a flush to complete
       */
      public static final ColumnFQ FLUSH_COLUMN = new ColumnFQ(NAME, new Text("flush"));
      /**
       * Holds compact IDs to enable waiting on a compaction to complete
       */
      public static final ColumnFQ COMPACT_COLUMN = new ColumnFQ(NAME, new Text("compact"));
      /**
       * Holds lock IDs to enable a sanity check to ensure that the TServer writing to the metadata tablet is not dead
       */
      public static final ColumnFQ LOCK_COLUMN = new ColumnFQ(NAME, new Text("lock"));
    }

    /**
     * Column family for storing entries created by the TServer to indicate it has loaded a tablet that it was assigned
     */
    public static class CurrentLocationColumnFamily {
      public static final Text NAME = new Text("loc");
    }

    /**
     * Column family for storing the assigned location
     */
    public static class FutureLocationColumnFamily {
      public static final Text NAME = new Text("future");
    }

    /**
     * Column family for storing last location, as a hint for assignment
     */
    public static class LastLocationColumnFamily {
      public static final Text NAME = new Text("last");
    }

    /**
     * Column family for storing suspension location, as a demand for assignment.
     */
    public static class SuspendLocationColumn {
      public static final ColumnFQ SUSPEND_COLUMN = new ColumnFQ(new Text("suspend"), new Text("loc"));
    }

    /**
     * Temporary markers that indicate a tablet loaded a bulk file
     */
    public static class BulkFileColumnFamily {
      public static final Text NAME = new Text("loaded");
    }

    /**
     * Temporary marker that indicates a tablet was successfully cloned
     */
    public static class ClonedColumnFamily {
      public static final Text NAME = new Text("!cloned");
    }

    /**
     * Column family for storing files used by a tablet
     */
    public static class DataFileColumnFamily {
      public static final Text NAME = new Text("file");
    }

    /**
     * Column family for storing the set of files scanned with an isolated scanner, to prevent them from being deleted
     */
    public static class ScanFileColumnFamily {
      public static final Text NAME = new Text("scan");
    }

    /**
     * Column family for storing write-ahead log entries
     */
    public static class LogColumnFamily {
      public static final Text NAME = new Text("log");
    }

    /**
     * Column family for indicating that the files in a tablet have been trimmed to only include data for the current tablet, so that they are safe to merge
     */
    public static class ChoppedColumnFamily {
      public static final Text NAME = new Text("chopped");
      public static final ColumnFQ CHOPPED_COLUMN = new ColumnFQ(NAME, new Text("chopped"));
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
    private static final Section section = new Section(RESERVED_PREFIX + "del", true, RESERVED_PREFIX + "dem", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }

  }

  /**
   * Holds bulk-load-in-progress processing flags
   */
  public static class BlipSection {
    private static final Section section = new Section(RESERVED_PREFIX + "blip", true, RESERVED_PREFIX + "bliq", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }

  }

  /**
   * Holds references to files that need replication
   * <p>
   * <code>~replhdfs://localhost:8020/accumulo/wal/tserver+port/WAL stat:local_table_id [] -&gt; protobuf</code>
   */
  public static class ReplicationSection {
    public static final Text COLF = new Text("stat");
    private static final ArrayByteSequence COLF_BYTE_SEQ = new ArrayByteSequence(COLF.toString());
    private static final Section section = new Section(RESERVED_PREFIX + "repl", true, RESERVED_PREFIX + "repm", false);

    public static Range getRange() {
      return section.getRange();
    }

    public static String getRowPrefix() {
      return section.getRowPrefix();
    }

    /**
     * Extract the table ID from the colfam
     *
     * @param k
     *          Key to extract from
     */
    public static Table.ID getTableId(Key k) {
      requireNonNull(k);
      return Table.ID.of(k.getColumnQualifier().toString());
    }

    /**
     * Extract the file name from the row suffix into the given {@link Text}
     *
     * @param k
     *          Key to extract from
     * @param buff
     *          Text to place file name into
     */
    public static void getFile(Key k, Text buff) {
      requireNonNull(k);
      requireNonNull(buff);
      checkArgument(COLF_BYTE_SEQ.equals(k.getColumnFamilyData()), "Given metadata replication status key with incorrect colfam");

      k.getRow(buff);

      buff.set(buff.getBytes(), section.getRowPrefix().length(), buff.getLength() - section.getRowPrefix().length());
    }
  }

}
