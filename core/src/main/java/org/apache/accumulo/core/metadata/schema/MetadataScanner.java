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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Table;
import org.apache.accumulo.core.clientImpl.Table.ID;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.FetchedColumns;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;

public class MetadataScanner implements Iterable<TabletMetadata>, AutoCloseable {

  public interface SourceOptions {
    TableOptions from(ClientContext ctx);

    TableOptions from(AccumuloClient client);
  }

  public interface TableOptions {
    RangeOptions scanRootTable();

    RangeOptions scanMetadataTable();

    RangeOptions scanTable(String tableName);
  }

  public interface RangeOptions {
    Options overTabletRange();

    Options overRange(Range range);

    Options overRange(Table.ID tableId);

    Options overRange(Table.ID tableId, Text startRow, Text endRow);
  }

  public interface Options {
    /**
     * Checks that the metadata table forms a linked list and automatically backs up until it does.
     */
    Options checkConsistency();

    /**
     * Saves the key values seen in the metadata table for each tablet.
     */
    Options saveKeyValues();

    Options fetchFiles();

    Options fetchLoaded();

    Options fetchLocation();

    Options fetchPrev();

    Options fetchLast();

    Options fetchScans();

    Options fetchDir();

    Options fetchTime();

    Options fetchCloned();

    MetadataScanner build()
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException;
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

  private static class Builder implements SourceOptions, TableOptions, RangeOptions, Options {

    private List<Text> families = new ArrayList<>();
    private List<ColumnFQ> qualifiers = new ArrayList<>();
    private AccumuloClient client;
    private String table = MetadataTable.NAME;
    private Range range;
    private EnumSet<FetchedColumns> fetchedCols = EnumSet.noneOf(FetchedColumns.class);
    private Text endRow;
    private boolean checkConsistency = false;
    private boolean saveKeyValues;

    @Override
    public Options fetchFiles() {
      fetchedCols.add(FetchedColumns.FILES);
      families.add(DataFileColumnFamily.NAME);
      return this;
    }

    @Override
    public Options fetchScans() {
      fetchedCols.add(FetchedColumns.SCANS);
      families.add(ScanFileColumnFamily.NAME);
      return this;
    }

    @Override
    public Options fetchLoaded() {
      fetchedCols.add(FetchedColumns.LOADED);
      families.add(BulkFileColumnFamily.NAME);
      return this;
    }

    @Override
    public Options fetchLocation() {
      fetchedCols.add(FetchedColumns.LOCATION);
      families.add(CurrentLocationColumnFamily.NAME);
      families.add(FutureLocationColumnFamily.NAME);
      return this;
    }

    @Override
    public Options fetchPrev() {
      fetchedCols.add(FetchedColumns.PREV_ROW);
      qualifiers.add(PREV_ROW_COLUMN);
      return this;
    }

    @Override
    public Options fetchDir() {
      fetchedCols.add(FetchedColumns.DIR);
      qualifiers.add(DIRECTORY_COLUMN);
      return this;
    }

    @Override
    public Options fetchLast() {
      fetchedCols.add(FetchedColumns.LAST);
      families.add(LastLocationColumnFamily.NAME);
      return this;
    }

    @Override
    public Options fetchTime() {
      fetchedCols.add(FetchedColumns.TIME);
      qualifiers.add(TIME_COLUMN);
      return this;
    }

    @Override
    public Options fetchCloned() {
      fetchedCols.add(FetchedColumns.CLONED);
      families.add(ClonedColumnFamily.NAME);
      return this;
    }

    @Override
    public MetadataScanner build()
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException {

      Scanner scanner = new IsolatedScanner(client.createScanner(table, Authorizations.EMPTY));
      scanner.setRange(range);

      if (checkConsistency && !fetchedCols.contains(FetchedColumns.PREV_ROW)) {
        fetchPrev();
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

      Iterable<TabletMetadata> tmi = TabletMetadata.convert(scanner, fetchedCols, checkConsistency,
          saveKeyValues);

      if (endRow != null) {
        // create an iterable that will stop at the tablet which contains the endRow
        return new MetadataScanner(scanner,
            () -> new TabletMetadataIterator(tmi.iterator(), endRow));
      } else {
        return new MetadataScanner(scanner, tmi);
      }
    }

    @Override
    public TableOptions from(ClientContext ctx) {
      this.client = ctx.getClient();
      return this;
    }

    @Override
    public TableOptions from(AccumuloClient client) {
      this.client = client;
      return this;
    }

    @Override
    public Options checkConsistency() {
      this.checkConsistency = true;
      return this;
    }

    @Override
    public Options saveKeyValues() {
      this.saveKeyValues = true;
      return this;
    }

    @Override
    public Options overTabletRange() {
      this.range = TabletsSection.getRange();
      return this;
    }

    @Override
    public Options overRange(Range range) {
      this.range = range;
      return this;
    }

    @Override
    public Options overRange(ID tableId) {
      this.range = TabletsSection.getRange(tableId);
      return this;
    }

    @Override
    public Options overRange(ID tableId, Text startRow, Text endRow) {
      this.range = new KeyExtent(tableId, null, startRow).toMetadataRange();
      this.endRow = endRow;
      return this;
    }

    @Override
    public RangeOptions scanTable(String tableName) {
      this.table = tableName;
      return this;
    }

    @Override
    public RangeOptions scanMetadataTable() {
      return scanTable(MetadataTable.NAME);
    }

    @Override
    public RangeOptions scanRootTable() {
      return scanTable(RootTable.NAME);
    }
  }

  private Scanner scanner;
  private Iterable<TabletMetadata> tablets;

  private MetadataScanner(Scanner scanner, Iterable<TabletMetadata> tmi) {
    this.scanner = scanner;
    this.tablets = tmi;
  }

  public static SourceOptions builder() {
    return new Builder();
  }

  @Override
  public Iterator<TabletMetadata> iterator() {
    return tablets.iterator();
  }

  public Stream<TabletMetadata> stream() {
    return StreamSupport.stream(tablets.spliterator(), false);
  }

  @Override
  public void close() {
    scanner.close();
  }
}
