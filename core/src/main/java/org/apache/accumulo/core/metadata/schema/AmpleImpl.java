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
import org.apache.accumulo.core.metadata.RootTable;
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
import org.apache.accumulo.core.metadata.schema.TabletsMetadata.Options;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

public class AmpleImpl implements Ample {
  private final AccumuloClient client;

  public AmpleImpl(AccumuloClient client) {
    this.client = client;
  }

  @Override
  public TabletMetadata readTablet(KeyExtent extent, ColumnType... colsToFetch) {
    Options builder = TabletsMetadata.builder().forTablet(extent);
    if (colsToFetch.length > 0)
      builder.fetch(colsToFetch);

    try (TabletsMetadata tablets = builder.build(client)) {
      return Iterables.getOnlyElement(tablets);
    }
  }

  @Override
  public AmpleImpl.Builder readTablets() {
    Builder builder = new Builder(client);
    return builder;
  }

  public static class Builder implements Iterable<TabletMetadata> {

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
    private AccumuloClient client;

    public Builder(AccumuloClient client) {
      this.client = client;
    }

    public Builder forTable(TableId tableID) {
      this.level = DataLevel.of(tableId);
      this.tableId = tableID;
      this.range = TabletsSection.getRange(tableId);
      return this;
    }

    public Builder overlapping(Text startRow, Text endRow) {
      this.range = new KeyExtent(tableId, null, startRow).toMetaRange();
      this.endRow = endRow;
      return this;
    }

    public Builder fetch(ColumnType... columnsToFetch) {
      Preconditions.checkArgument(columnsToFetch.length > 0);

      for (ColumnType colToFetch : columnsToFetch) {

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

    public Builder build() {
      Preconditions.checkState(level == null ^ table == null);
      if (level == DataLevel.ROOT) {
        ClientContext ctx = ((ClientContext) this.client);
        ZooCache zc = ctx.getZooCache();
        String zkRoot = ctx.getZooKeeperRoot();
        return new Builder(getRootMetadata(zkRoot, zc));
      } else {
        return buildNonRoot(this.client);
      }
    }

    private Builder buildNonRoot(AccumuloClient client) {
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
          return new Builder(scanner, () -> new TabletMetadataIterator(tmi.iterator(), endRow));
        } else {
          return new Builder(scanner, tmi);
        }
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    public static TabletMetadata getRootMetadata(String zkRoot, ZooCache zc) {
      return RootTabletMetadata.fromJson(zc.get(zkRoot + RootTable.ZROOT_TABLET))
          .convertToTabletMetadata();
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

    private Scanner scanner;

    private Iterable<TabletMetadata> tablets;

    private Builder(TabletMetadata tm) {
      this.scanner = null;
      this.tablets = Collections.singleton(tm);
    }

    private Builder(Scanner scanner, Iterable<TabletMetadata> tmi) {
      this.scanner = scanner;
      this.tablets = tmi;
    }

    public void close() {
      if (scanner != null) {
        scanner.close();
      }
    }

    public Iterator<TabletMetadata> iterator() {
      return tablets.iterator();
    }

    public Stream<TabletMetadata> stream() {
      return StreamSupport.stream(tablets.spliterator(), false);
    }
  }
}
