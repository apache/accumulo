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

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Function;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;

public class TabletMetadata {

  private Table.ID tableId;
  private Text prevEndRow;
  private boolean sawPrevEndRow = false;
  private Text endRow;
  private Location location;
  private List<String> files;
  private List<String> scans;
  private Set<String> loadedFiles;
  private EnumSet<FetchedColumns> fetchedCols;
  private KeyExtent extent;
  private Location last;
  private String dir;
  private String time;
  private String cloned;
  private SortedMap<Key,Value> keyValues;

  public static enum LocationType {
    CURRENT, FUTURE, LAST
  }

  public static enum FetchedColumns {
    LOCATION, PREV_ROW, FILES, LAST, LOADED, SCANS, DIR, TIME, CLONED
  }

  public static class Location {
    private final String server;
    private final String session;
    private final LocationType lt;

    Location(String server, String session, LocationType lt) {
      this.server = server;
      this.session = session;
      this.lt = lt;
    }

    public HostAndPort getHostAndPort() {
      return HostAndPort.fromString(server);
    }

    public String getSession() {
      return session;
    }

    public LocationType getLocationType() {
      return lt;
    }
  }

  public Table.ID getTableId() {
    return tableId;
  }

  public KeyExtent getExtent() {
    if (extent == null) {
      extent = new KeyExtent(getTableId(), getEndRow(), getPrevEndRow());
    }
    return extent;
  }

  private void ensureFetched(FetchedColumns col) {
    Preconditions.checkState(fetchedCols.contains(col), "%s was not fetched", col);
  }

  public Text getPrevEndRow() {
    ensureFetched(FetchedColumns.PREV_ROW);
    return prevEndRow;
  }

  public boolean sawPrevEndRow() {
    ensureFetched(FetchedColumns.PREV_ROW);
    return sawPrevEndRow;
  }

  public Text getEndRow() {
    return endRow;
  }

  public Location getLocation() {
    ensureFetched(FetchedColumns.LOCATION);
    return location;
  }

  public Set<String> getLoaded() {
    ensureFetched(FetchedColumns.LOADED);
    return loadedFiles;
  }

  public Location getLast() {
    ensureFetched(FetchedColumns.LAST);
    return last;
  }

  public List<String> getFiles() {
    ensureFetched(FetchedColumns.FILES);
    return files;
  }

  public List<String> getScans() {
    ensureFetched(FetchedColumns.SCANS);
    return scans;
  }

  public String getDir() {
    ensureFetched(FetchedColumns.DIR);
    return dir;
  }

  public String getTime() {
    ensureFetched(FetchedColumns.TIME);
    return time;
  }

  public String getCloned() {
    ensureFetched(FetchedColumns.CLONED);
    return cloned;
  }

  public SortedMap<Key,Value> getKeyValues() {
    Preconditions.checkState(keyValues != null, "Requested key values when it was not saved");
    return keyValues;
  }

  private static TabletMetadata convertRow(Iterator<Entry<Key,Value>> rowIter,
      EnumSet<FetchedColumns> fetchedColumns, boolean buildKeyValueMap) {
    Objects.requireNonNull(rowIter);

    TabletMetadata te = new TabletMetadata();
    ImmutableSortedMap.Builder<Key,Value> kvBuilder = null;
    if (buildKeyValueMap) {
      kvBuilder = ImmutableSortedMap.naturalOrder();
    }

    Builder<String> filesBuilder = ImmutableList.builder();
    Builder<String> scansBuilder = ImmutableList.builder();
    final ImmutableSet.Builder<String> loadedFilesBuilder = ImmutableSet.builder();
    ByteSequence row = null;

    while (rowIter.hasNext()) {
      Entry<Key,Value> kv = rowIter.next();
      Key k = kv.getKey();
      Value v = kv.getValue();
      Text fam = k.getColumnFamily();

      if (buildKeyValueMap) {
        kvBuilder.put(k, v);
      }

      if (row == null) {
        row = k.getRowData();
        KeyExtent ke = new KeyExtent(k.getRow(), (Text) null);
        te.endRow = ke.getEndRow();
        te.tableId = ke.getTableId();
      } else if (!row.equals(k.getRowData())) {
        throw new IllegalArgumentException(
            "Input contains more than one row : " + row + " " + k.getRowData());
      }

      switch (fam.toString()) {
        case TabletColumnFamily.STR_NAME:
          if (PREV_ROW_COLUMN.hasColumns(k)) {
            te.prevEndRow = KeyExtent.decodePrevEndRow(v);
            te.sawPrevEndRow = true;
          }
          break;
        case ServerColumnFamily.STR_NAME:
          if (DIRECTORY_COLUMN.hasColumns(k)) {
            te.dir = v.toString();
          } else if (TIME_COLUMN.hasColumns(k)) {
            te.time = v.toString();
          }
          break;
        case DataFileColumnFamily.STR_NAME:
          filesBuilder.add(k.getColumnQualifier().toString());
          break;
        case BulkFileColumnFamily.STR_NAME:
          loadedFilesBuilder.add(k.getColumnQualifier().toString());
          break;
        case CurrentLocationColumnFamily.STR_NAME:
          if (te.location != null) {
            throw new IllegalArgumentException(
                "Input contains more than one location " + te.location + " " + v);
          }
          te.location = new Location(v.toString(), k.getColumnQualifierData().toString(),
              LocationType.CURRENT);
          break;
        case FutureLocationColumnFamily.STR_NAME:
          if (te.location != null) {
            throw new IllegalArgumentException(
                "Input contains more than one location " + te.location + " " + v);
          }
          te.location = new Location(v.toString(), k.getColumnQualifierData().toString(),
              LocationType.FUTURE);
          break;
        case LastLocationColumnFamily.STR_NAME:
          te.last = new Location(v.toString(), k.getColumnQualifierData().toString(),
              LocationType.LAST);
          break;
        case ScanFileColumnFamily.STR_NAME:
          scansBuilder.add(k.getColumnQualifierData().toString());
          break;
        case ClonedColumnFamily.STR_NAME:
          te.cloned = v.toString();
          break;
        default:
          throw new IllegalStateException("Unexpected family " + fam);
      }
    }

    te.files = filesBuilder.build();
    te.loadedFiles = loadedFilesBuilder.build();
    te.fetchedCols = fetchedColumns;
    te.scans = scansBuilder.build();
    if (buildKeyValueMap) {
      te.keyValues = kvBuilder.build();
    }
    return te;
  }

  static Iterable<TabletMetadata> convert(Scanner input, EnumSet<FetchedColumns> fetchedColumns,
      boolean checkConsistency, boolean buildKeyValueMap) {

    Range range = input.getRange();

    Function<Range,Iterator<TabletMetadata>> iterFactory = r -> {
      synchronized (input) {
        input.setRange(r);
        RowIterator rowIter = new RowIterator(input);
        return Iterators.transform(rowIter, ri -> convertRow(ri, fetchedColumns, buildKeyValueMap));
      }
    };

    if (checkConsistency) {
      return () -> new LinkingIterator(iterFactory, range);
    } else {
      return () -> iterFactory.apply(range);
    }
  }

  @VisibleForTesting
  static TabletMetadata create(String id, String prevEndRow, String endRow) {
    TabletMetadata te = new TabletMetadata();
    te.tableId = Table.ID.of(id);
    te.sawPrevEndRow = true;
    te.prevEndRow = prevEndRow == null ? null : new Text(prevEndRow);
    te.endRow = endRow == null ? null : new Text(endRow);
    te.fetchedCols = EnumSet.of(FetchedColumns.PREV_ROW);
    return te;
  }
}
