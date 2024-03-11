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

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.ReadConsistency;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

/**
 * An abstraction layer for reading tablet metadata from the accumulo.metadata and accumulo.root
 * tables.
 */
public class TabletsMetadata implements Iterable<TabletMetadata>, AutoCloseable {

  public static class Builder implements TableRangeOptions, TableOptions, RangeOptions, Options {

    private final List<Text> families = new ArrayList<>();
    private final List<ColumnFQ> qualifiers = new ArrayList<>();
    private Set<KeyExtent> extentsToFetch = null;
    private Ample.DataLevel level;
    private String table;
    private Range range;
    private EnumSet<ColumnType> fetchedCols = EnumSet.noneOf(ColumnType.class);
    private Text endRow;
    private boolean checkConsistency = false;
    private boolean saveKeyValues;
    private TableId tableId;
    private ReadConsistency readConsistency = ReadConsistency.IMMEDIATE;
    private final AccumuloClient _client;
    private Collection<KeyExtent> extents = null;

    Builder(AccumuloClient client) {
      this._client = client;
    }

    @Override
    public TabletsMetadata build() {
      if (extents != null) {
        // setting multiple extents with forTablets(extents) is mutually exclusive with these
        // single-tablet options
        checkState(range == null && table == null && level == null && !checkConsistency);
        return buildExtents(_client);
      }

      checkState((level == null) != (table == null),
          "scanTable() cannot be used in conjunction with forLevel(), forTable() or forTablet()");
      if (level == DataLevel.ROOT) {
        ClientContext ctx = ((ClientContext) _client);
        return new TabletsMetadata(getRootMetadata(ctx, readConsistency));
      } else {
        return buildNonRoot(_client);
      }
    }

    private TabletsMetadata buildExtents(AccumuloClient client) {

      Map<DataLevel,List<KeyExtent>> groupedExtents =
          extents.stream().collect(groupingBy(ke -> DataLevel.of(ke.tableId())));

      List<Iterable<TabletMetadata>> iterables = new ArrayList<>();

      List<AutoCloseable> closables = new ArrayList<>();

      Preconditions.checkState(extentsToFetch != null);

      if (!fetchedCols.isEmpty()) {
        fetch(ColumnType.PREV_ROW);
      }

      for (DataLevel level : groupedExtents.keySet()) {
        if (level == DataLevel.ROOT) {
          iterables.add(() -> Iterators
              .singletonIterator(getRootMetadata((ClientContext) client, readConsistency)));
        } else {
          try {
            BatchScanner scanner =
                client.createBatchScanner(level.metaTable(), Authorizations.EMPTY);

            var ranges =
                groupedExtents.get(level).stream().map(KeyExtent::toMetaRange).collect(toList());
            scanner.setRanges(ranges);

            configureColumns(scanner);
            IteratorSetting iterSetting = new IteratorSetting(100, WholeRowIterator.class);
            scanner.addScanIterator(iterSetting);

            Iterable<TabletMetadata> tmi = () -> Iterators.transform(scanner.iterator(), entry -> {
              try {
                return TabletMetadata.convertRow(WholeRowIterator
                    .decodeRow(entry.getKey(), entry.getValue()).entrySet().iterator(), fetchedCols,
                    saveKeyValues);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            });

            iterables.add(tmi);
            closables.add(scanner);

          } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
          }

        }
      }

      return new TabletsMetadata(() -> {
        for (AutoCloseable closable : closables) {
          closable.close();
        }
      }, () -> iterables.stream().flatMap(i -> StreamSupport.stream(i.spliterator(), false))
          .filter(tabletMetadata -> extentsToFetch.contains(tabletMetadata.getExtent()))
          .iterator());

    }

    private TabletsMetadata buildNonRoot(AccumuloClient client) {
      try {

        String resolvedTable = table == null ? level.metaTable() : table;

        Scanner scanner =
            new IsolatedScanner(client.createScanner(resolvedTable, Authorizations.EMPTY));
        scanner.setRange(range);

        boolean extentsPresent = extentsToFetch != null;

        if (!fetchedCols.isEmpty() && (checkConsistency || extentsPresent)) {
          fetch(ColumnType.PREV_ROW);
        }

        configureColumns(scanner);
        Range range1 = scanner.getRange();

        Function<Range,Iterator<TabletMetadata>> iterFactory = r -> {
          synchronized (scanner) {
            scanner.setRange(r);
            RowIterator rowIter = new RowIterator(scanner);
            Iterator<TabletMetadata> iter = Iterators.transform(rowIter,
                ri -> TabletMetadata.convertRow(ri, fetchedCols, saveKeyValues));
            if (extentsPresent) {
              return Iterators.filter(iter,
                  tabletMetadata -> extentsToFetch.contains(tabletMetadata.getExtent()));
            } else {
              return iter;
            }
          }
        };

        Iterable<TabletMetadata> tmi;
        if (checkConsistency) {
          tmi = () -> new LinkingIterator(iterFactory, range1);
        } else {
          tmi = () -> iterFactory.apply(range1);
        }

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

    private void configureColumns(ScannerBase scanner) {
      families.forEach(scanner::fetchColumnFamily);
      qualifiers.forEach(col -> col.fetch(scanner));
      if (families.isEmpty() && qualifiers.isEmpty()) {
        fetchedCols = EnumSet.allOf(ColumnType.class);
      }
    }

    @Override
    public Options checkConsistency() {
      checkState(extents == null, "Unable to check consistency of non-contiguous tablets");
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
          case SUSPEND:
            families.add(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily());
            break;
          case TIME:
            qualifiers.add(TIME_COLUMN);
            break;
          case ECOMP:
            families.add(ExternalCompactionColumnFamily.NAME);
            break;
          default:
            throw new IllegalArgumentException("Unknown col type " + colToFetch);

        }
      }

      return this;
    }

    /**
     * For a given data level, read all of its tablets metadata. For {@link DataLevel#USER} this
     * will read tablet metadata from the accumulo.metadata table for all user tables. For
     * {@link DataLevel#METADATA} this will read tablet metadata from the accumulo.root table. For
     * {@link DataLevel#ROOT} this will read tablet metadata from Zookeeper.
     */
    @Override
    public Options forLevel(DataLevel level) {
      this.level = level;
      this.range = TabletsSection.getRange();
      return this;
    }

    /**
     * For a given table read all of its tablet metadata. If the table id is for a user table, then
     * its metadata will be read from its section in the accumulo.metadata table. If the table id is
     * for the accumulo.metadata table, then its metadata will be read from the accumulo.root table.
     * If the table id is for the accumulo.root table, then its metadata will be read from
     * zookeeper.
     */
    @Override
    public TableRangeOptions forTable(TableId tableId) {
      this.level = DataLevel.of(tableId);
      this.tableId = tableId;
      this.range = TabletsSection.getRange(tableId);
      return this;
    }

    @Override
    public Options forTablet(KeyExtent extent) {
      forTable(extent.tableId());
      this.range = new Range(extent.toMetaRow());
      this.extentsToFetch = Set.of(extent);
      return this;
    }

    @Override
    public Options forTablets(Collection<KeyExtent> extents) {
      this.level = null;
      this.extents = extents;
      this.extentsToFetch = Set.copyOf(extents);
      return this;
    }

    @Override
    public Options overRange(Range range) {
      this.range = TabletsSection.getRange().clip(range);
      return this;
    }

    @Override
    public Options overlapping(Text startRow, boolean startInclusive, Text endRow) {
      var metaStartRow =
          TabletsSection.encodeRow(tableId, startRow == null ? new Text("") : startRow);
      var metaEndRow = TabletsSection.encodeRow(tableId, null);
      this.range =
          new Range(metaStartRow, startRow == null ? true : startInclusive, metaEndRow, true);
      this.endRow = endRow;

      return this;
    }

    @Override
    public Options overlapping(Text startRow, Text endRow) {
      return overlapping(startRow, false, endRow);
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

    @Override
    public Options readConsistency(ReadConsistency readConsistency) {
      this.readConsistency = Objects.requireNonNull(readConsistency);
      return this;
    }
  }

  public interface Options {

    TabletsMetadata build();

    /**
     * Checks that the metadata table forms a linked list and automatically backs up until it does.
     */
    Options checkConsistency();

    Options fetch(ColumnType... columnsToFetch);

    /**
     * Saves the key values seen in the metadata table for each tablet.
     */
    Options saveKeyValues();

    /**
     * Controls how the data is read. If not, set then the default is
     * {@link ReadConsistency#IMMEDIATE}
     */
    Options readConsistency(ReadConsistency readConsistency);
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
     * tablet where the end row and prev end row exactly match the given extent.
     */
    Options forTablet(KeyExtent extent);

    /**
     * Get the tablet metadata for the given extents. This will only return tablets where the end
     * row and prev end row exactly match the given extents.
     */
    Options forTablets(Collection<KeyExtent> extents);

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
     *
     * <p>
     * This method is equivalent to calling {@link #overlapping(Text, boolean, Text)} as
     * {@code overlapping(startRow, false, endRow)}
     * </p>
     */
    Options overlapping(Text startRow, Text endRow);

    /**
     * When {@code startRowInclusive} is true limits to tablets that overlap the range
     * {@code [startRow,endRow]}. When {@code startRowInclusive} is false limits to tablets that
     * overlap the range {@code (startRow, endRow]}. Can pass null for start and end row
     * representing -inf and +inf.
     */
    Options overlapping(Text startRow, boolean startRowInclusive, Text endRow);
  }

  private static class TabletMetadataIterator implements Iterator<TabletMetadata> {

    private boolean sawLast = false;
    private final Iterator<TabletMetadata> iter;
    private final Text endRow;

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

  public static TableOptions builder(AccumuloClient client) {
    return new Builder(client);
  }

  private static TabletMetadata getRootMetadata(ClientContext ctx,
      ReadConsistency readConsistency) {
    String zkRoot = ctx.getZooKeeperRoot();
    switch (readConsistency) {
      case EVENTUAL:
        return getRootMetadata(zkRoot, ctx.getZooCache());
      case IMMEDIATE:
        ZooReader zooReader = ctx.getZooReader();
        try {
          var path = zkRoot + RootTable.ZROOT_TABLET;
          // attempt (see ZOOKEEPER-1675) to ensure the latest root table metadata is read from
          // zookeeper
          zooReader.sync(path);
          byte[] bytes = zooReader.getData(path);
          return new RootTabletMetadata(new String(bytes, UTF_8)).toTabletMetadata();
        } catch (InterruptedException | KeeperException e) {
          throw new RuntimeException(e);
        }
      default:
        throw new IllegalArgumentException("Unknown consistency level " + readConsistency);
    }
  }

  public static TabletMetadata getRootMetadata(String zkRoot, ZooCache zc) {
    byte[] jsonBytes = zc.get(zkRoot + RootTable.ZROOT_TABLET);
    return new RootTabletMetadata(new String(jsonBytes, UTF_8)).toTabletMetadata();
  }

  private final AutoCloseable closeable;

  private final Iterable<TabletMetadata> tablets;

  private TabletsMetadata(TabletMetadata tm) {
    this.closeable = null;
    this.tablets = Collections.singleton(tm);
  }

  private TabletsMetadata(AutoCloseable closeable, Iterable<TabletMetadata> tmi) {
    this.closeable = closeable;
    this.tablets = tmi;
  }

  @Override
  public void close() {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (RuntimeException e) {
        // avoid wrapping runtime w/ runtime
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Iterator<TabletMetadata> iterator() {
    return tablets.iterator();
  }

  public Stream<TabletMetadata> stream() {
    return StreamSupport.stream(tablets.spliterator(), false).onClose(this::close);
  }
}
