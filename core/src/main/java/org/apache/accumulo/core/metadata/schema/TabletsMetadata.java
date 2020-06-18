/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.metadata.schema;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * An abstraction layer for reading tablet metadata from the accumulo.metadata and accumulo.root
 * tables.
 */
public class TabletsMetadata implements Iterable<TabletMetadata>, AutoCloseable {

  private static class Builder implements TableRangeOptions, TableOptions, RangeOptions, Options {

    private List<Text> families = new ArrayList<>();
    private List<ColumnFQ> qualifiers = new ArrayList<>();
    private Ample.DataLevel level;
    private String table;
    private Range range;
    private EnumSet<ColumnType> fetchedCols = EnumSet.noneOf(ColumnType.class);
    private Text endRow;
    private boolean checkConsistency = false;
    private boolean saveKeyValues;
    private TableId tableId;

    @Override
    public TabletsMetadata build(AccumuloClient client) {
      Preconditions.checkState(level == null ^ table == null);
      if (level == DataLevel.ROOT) {
        ClientContext ctx = ((ClientContext) client);
        ZooCache zc = ctx.getZooCache();
        String zkRoot = ctx.getZooKeeperRoot();
        return new TabletsMetadata(getRootMetadata(zkRoot, zc));
      } else {
        return buildNonRoot(client);
      }
    }

    private TabletsMetadata buildNonRoot(AccumuloClient client) {
      try {

        String resolvedTable = table == null ? level.metaTable() : table;

        Scanner scanner =
            new IsolatedScanner(client.createScanner(resolvedTable, Authorizations.EMPTY));
        scanner.setRange(range);

        if (checkConsistency && !fetchedCols.contains(ColumnType.PREV_ROW)) {
          fetch(ColumnType.PREV_ROW);
        }

        for (Text fam : families) {
          scanner.fetchColumnFamily(fam);
        }

        for (ColumnFQ col : qualifiers) {
          col.fetch(scanner);
        }

        if (families.isEmpty() && qualifiers.isEmpty()) {
          fetchedCols = EnumSet.allOf(ColumnType.class);
        }

        Iterable<TabletMetadata> tmi =
            TabletMetadata.convert(scanner, fetchedCols, checkConsistency, saveKeyValues);

        if (endRow != null) {
          // create an iterable that will stop at the tablet which contains the endRow
          return new TabletsMetadata(scanner,
              () -> new TabletMetadataIterator(tmi.iterator(), endRow));
        } else {
          return new TabletsMetadata(scanner, tmi);
        }
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Options checkConsistency() {
      this.checkConsistency = true;
      return this;
    }

    @Override
    public Options fetch(ColumnType... colsToFetch) {
      Preconditions.checkArgument(colsToFetch.length > 0);

      for (ColumnType colToFetch : colsToFetch) {

        fetchedCols.add(colToFetch);

        switch (colToFetch) {
          case CLONED:
            families.add(ClonedColumnFamily.NAME);
            break;
          case COMPACT_ID:
            qualifiers.add(COMPACT_COLUMN);
            break;
          case DIR:
            qualifiers.add(DIRECTORY_COLUMN);
            break;
          case FILES:
            families.add(DataFileColumnFamily.NAME);
            break;
          case FLUSH_ID:
            qualifiers.add(FLUSH_COLUMN);
            break;
          case LAST:
            families.add(LastLocationColumnFamily.NAME);
            break;
          case LOADED:
            families.add(BulkFileColumnFamily.NAME);
            break;
          case LOCATION:
            families.add(CurrentLocationColumnFamily.NAME);
            families.add(FutureLocationColumnFamily.NAME);
            break;
          case LOGS:
            families.add(LogColumnFamily.NAME);
            break;
          case PREV_ROW:
            qualifiers.add(PREV_ROW_COLUMN);
            break;
          case SCANS:
            families.add(ScanFileColumnFamily.NAME);
            break;
          case TIME:
            qualifiers.add(TIME_COLUMN);
            break;
          default:
            throw new IllegalArgumentException("Unknown col type " + colToFetch);

        }
      }

      return this;
    }

    @Override
    public Options forLevel(DataLevel level) {
      this.level = level;
      this.range = TabletsSection.getRange();
      return this;
    }

    @Override
    public TableRangeOptions forTable(TableId tableId) {
      this.level = DataLevel.of(tableId);
      this.tableId = tableId;
      this.range = TabletsSection.getRange(tableId);
      return this;
    }

    @Override
    public Options forTablet(KeyExtent extent) {
      forTable(extent.getTableId());
      this.range = new Range(extent.getMetadataEntry());
      return this;
    }

    @Override
    public Options overRange(Range range) {
      this.range = TabletsSection.getRange().clip(range);
      return this;
    }

    @Override
    public Options overlapping(Text startRow, Text endRow) {
      this.range = new KeyExtent(tableId, null, startRow).toMetadataRange();
      this.endRow = endRow;
      return this;
    }

    @Override
    public Options saveKeyValues() {
      this.saveKeyValues = true;
      return this;
    }

    @Override
    public RangeOptions scanTable(String tableName) {
      this.table = tableName;
      this.range = TabletsSection.getRange();
      return this;
    }
  }

  public interface Options {
    TabletsMetadata build(AccumuloClient client);

    /**
     * Checks that the metadata table forms a linked list and automatically backs up until it does.
     */
    Options checkConsistency();

    Options fetch(ColumnType... columnsToFetch);

    /**
     * Saves the key values seen in the metadata table for each tablet.
     */
    Options saveKeyValues();
  }

  public interface RangeOptions extends Options {
    Options overRange(Range range);
  }

  public interface TableOptions {

    /**
     * Read all of the tablet metadata for this level.
     */
    Options forLevel(DataLevel level);

    /**
     * Get the tablet metadata for this extents end row. This should only ever return a single
     * tablet. No checking is done for prev row, so it could differ.
     */
    Options forTablet(KeyExtent extent);

    /**
     * This method automatically determines where the metadata for the passed in table ID resides.
     * For example if a user tablet ID is passed in, then the metadata table is scanned. If the
     * metadata table ID is passed in then the root table is scanned. Defaults to returning all
     * tablets for the table ID.
     */
    TableRangeOptions forTable(TableId tableId);

    /**
     * Obtain tablet metadata by scanning the metadata table. Defaults to the range
     * {@link TabletsSection#getRange()}
     */
    default RangeOptions scanMetadataTable() {
      return scanTable(MetadataTable.NAME);
    }

    /**
     * Obtain tablet metadata by scanning an arbitrary table. Defaults to the range
     * {@link TabletsSection#getRange()}
     */
    RangeOptions scanTable(String tableName);
  }

  public interface TableRangeOptions extends Options {

    /**
     * @see #overlapping(Text, Text)
     */
    default Options overlapping(byte[] startRow, byte[] endRow) {
      return overlapping(startRow == null ? null : new Text(startRow),
          endRow == null ? null : new Text(endRow));
    }

    /**
     * Limit to tablets that overlap the range {@code (startRow, endRow]}. Can pass null
     * representing -inf and +inf. The impl creates open ended ranges which may be problematic, see
     * #813.
     */
    Options overlapping(Text startRow, Text endRow);
  }

  private static class TabletMetadataIterator implements Iterator<TabletMetadata> {

    private boolean sawLast = false;
    private Iterator<TabletMetadata> iter;
    private Text endRow;

    TabletMetadataIterator(Iterator<TabletMetadata> source, Text endRow) {
      this.iter = source;
      this.endRow = endRow;
    }

    @Override
    public boolean hasNext() {
      return !sawLast && iter.hasNext();
    }

    @Override
    public TabletMetadata next() {
      if (sawLast) {
        throw new NoSuchElementException();
      }
      TabletMetadata next = iter.next();
      // impossible to construct a range that stops at the first tablet that contains endRow. That
      // is why this specialized code exists.
      if (next.getExtent().contains(endRow)) {
        sawLast = true;
      }
      return next;
    }
  }

  public static TableOptions builder() {
    return new Builder();
  }

  public static TabletMetadata getRootMetadata(String zkRoot, ZooCache zc) {
    return RootTabletMetadata.fromJson(zc.get(zkRoot + RootTable.ZROOT_TABLET))
        .convertToTabletMetadata();
  }

  private Scanner scanner;

  private Iterable<TabletMetadata> tablets;

  private TabletsMetadata(TabletMetadata tm) {
    this.scanner = null;
    this.tablets = Collections.singleton(tm);
  }

  private TabletsMetadata(Scanner scanner, Iterable<TabletMetadata> tmi) {
    this.scanner = scanner;
    this.tablets = tmi;
  }

  @Override
  public void close() {
    if (scanner != null) {
      scanner.close();
    }
  }

  @Override
  public Iterator<TabletMetadata> iterator() {
    return tablets.iterator();
  }

  public Stream<TabletMetadata> stream() {
    return StreamSupport.stream(tablets.spliterator(), false);
  }
}
