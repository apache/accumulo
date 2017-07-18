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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.FetchedColumns;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class MetadataScanner {

  public static interface SourceOptions {
    TableOptions from(Scanner scanner);

    TableOptions from(ClientContext ctx);
  }

  public static interface TableOptions {
    ColumnOptions overRootTable();

    ColumnOptions overMetadataTable();

    ColumnOptions overUserTableId(Table.ID tableId);

    ColumnOptions overUserTableId(Table.ID tableId, Text startRow, Text endRow);
  }

  public static interface ColumnOptions {
    public ColumnOptions fetchFiles();

    public ColumnOptions fetchLocation();

    public ColumnOptions fetchPrev();

    public ColumnOptions fetchLast();

    public Iterable<TabletMetadata> build() throws TableNotFoundException, AccumuloException, AccumuloSecurityException;
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
      if (next.getExtent().contains(endRow)) {
        sawLast = true;
      }
      return next;
    }
  }

  private static class Builder implements SourceOptions, TableOptions, ColumnOptions {

    private List<Text> families = new ArrayList<>();
    private List<ColumnFQ> qualifiers = new ArrayList<>();
    private Scanner scanner;
    private ClientContext ctx;
    private String table;
    private Table.ID userTableId;
    private EnumSet<FetchedColumns> fetchedCols = EnumSet.noneOf(FetchedColumns.class);
    private Text startRow;
    private Text endRow;

    @Override
    public ColumnOptions fetchFiles() {
      fetchedCols.add(FetchedColumns.FILES);
      families.add(DataFileColumnFamily.NAME);
      return this;
    }

    @Override
    public ColumnOptions fetchLocation() {
      fetchedCols.add(FetchedColumns.LOCATION);
      families.add(CurrentLocationColumnFamily.NAME);
      families.add(FutureLocationColumnFamily.NAME);
      return this;
    }

    @Override
    public ColumnOptions fetchPrev() {
      fetchedCols.add(FetchedColumns.PREV_ROW);
      qualifiers.add(PREV_ROW_COLUMN);
      return this;
    }

    @Override
    public ColumnOptions fetchLast() {
      fetchedCols.add(FetchedColumns.LAST);
      families.add(LastLocationColumnFamily.NAME);
      return this;
    }

    @Override
    public Iterable<TabletMetadata> build() throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
      if (ctx != null) {
        scanner = new IsolatedScanner(ctx.getConnector().createScanner(table, Authorizations.EMPTY));
      } else if (!(scanner instanceof IsolatedScanner)) {
        scanner = new IsolatedScanner(scanner);
      }

      if (userTableId != null) {
        scanner.setRange(new KeyExtent(userTableId, null, startRow).toMetadataRange());
      }

      for (Text fam : families) {
        scanner.fetchColumnFamily(fam);
      }

      for (ColumnFQ col : qualifiers) {
        col.fetch(scanner);
      }

      if (families.size() == 0 && qualifiers.size() == 0) {
        fetchedCols = EnumSet.allOf(FetchedColumns.class);
      }

      Iterable<TabletMetadata> tmi = TabletMetadata.convert(scanner, fetchedCols);

      if (endRow != null) {
        // create an iterable that will stop at the tablet which contains the endRow
        return new Iterable<TabletMetadata>() {
          @Override
          public Iterator<TabletMetadata> iterator() {
            return new TabletMetadataIterator(tmi.iterator(), endRow);
          }
        };
      } else {
        return tmi;
      }

    }

    @Override
    public ColumnOptions overRootTable() {
      this.table = RootTable.NAME;
      return this;
    }

    @Override
    public ColumnOptions overMetadataTable() {
      this.table = MetadataTable.NAME;
      return this;
    }

    @Override
    public ColumnOptions overUserTableId(Table.ID tableId) {
      Preconditions.checkArgument(!tableId.equals(RootTable.ID) && !tableId.equals(MetadataTable.ID));

      this.table = MetadataTable.NAME;
      this.userTableId = tableId;
      return this;
    }

    @Override
    public TableOptions from(Scanner scanner) {
      this.scanner = scanner;
      return this;
    }

    @Override
    public TableOptions from(ClientContext ctx) {
      this.ctx = ctx;
      return this;
    }

    @Override
    public ColumnOptions overUserTableId(Table.ID tableId, Text startRow, Text endRow) {
      this.table = MetadataTable.NAME;
      this.userTableId = tableId;
      this.startRow = startRow;
      this.endRow = endRow;
      return this;
    }

  }

  public static SourceOptions builder() {
    return new Builder();
  }

}
