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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.OLD_PREV_ROW_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.SPLIT_RATIO_QUAL;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.function.Function;

import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterators;

public class TabletMetadata {

  private TableId tableId;
  private Text prevEndRow;
  private boolean sawPrevEndRow = false;
  private Text oldPrevEndRow;
  private boolean sawOldPrevEndRow = false;
  private Text endRow;
  private Location location;
  private Map<StoredTabletFile,DataFileValue> files;
  private List<StoredTabletFile> scans;
  private Map<TabletFile,Long> loadedFiles;
  private EnumSet<ColumnType> fetchedCols;
  private KeyExtent extent;
  private Location last;
  private String dirName;
  private MetadataTime time;
  private String cloned;
  private SortedMap<Key,Value> keyValues;
  private OptionalLong flush = OptionalLong.empty();
  private List<LogEntry> logs;
  private OptionalLong compact = OptionalLong.empty();
  private Double splitRatio = null;

  public enum LocationType {
    CURRENT, FUTURE, LAST
  }

  public enum ColumnType {
    LOCATION,
    PREV_ROW,
    OLD_PREV_ROW,
    FILES,
    LAST,
    LOADED,
    SCANS,
    DIR,
    TIME,
    CLONED,
    FLUSH_ID,
    LOGS,
    COMPACT_ID,
    SPLIT_RATIO
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

    public LocationType getType() {
      return lt;
    }

    @Override
    public String toString() {
      return server + ":" + session + ":" + lt;
    }

    /**
     * @return an ID composed of host, port, and session id.
     */
    public String getId() {
      return server + "[" + session + "]";
    }
  }

  public TableId getTableId() {
    return tableId;
  }

  public KeyExtent getExtent() {
    if (extent == null) {
      extent = new KeyExtent(getTableId(), getEndRow(), getPrevEndRow());
    }
    return extent;
  }

  private void ensureFetched(ColumnType col) {
    Preconditions.checkState(fetchedCols.contains(col), "%s was not fetched", col);
  }

  public Text getPrevEndRow() {
    ensureFetched(ColumnType.PREV_ROW);
    if (!sawPrevEndRow) {
      throw new IllegalStateException(
          "No prev endrow seen.  tableId: " + tableId + " endrow: " + endRow);
    }
    return prevEndRow;
  }

  public boolean sawPrevEndRow() {
    ensureFetched(ColumnType.PREV_ROW);
    return sawPrevEndRow;
  }

  public Text getOldPrevEndRow() {
    ensureFetched(ColumnType.OLD_PREV_ROW);
    if (!sawOldPrevEndRow) {
      throw new IllegalStateException(
          "No old prev endrow seen.  tableId: " + tableId + " endrow: " + endRow);
    }
    return oldPrevEndRow;
  }

  public boolean sawOldPrevEndRow() {
    ensureFetched(ColumnType.OLD_PREV_ROW);
    return sawOldPrevEndRow;
  }

  public Text getEndRow() {
    return endRow;
  }

  public Location getLocation() {
    ensureFetched(ColumnType.LOCATION);
    return location;
  }

  public boolean hasCurrent() {
    ensureFetched(ColumnType.LOCATION);
    return location != null && location.getType() == LocationType.CURRENT;
  }

  public Map<TabletFile,Long> getLoaded() {
    ensureFetched(ColumnType.LOADED);
    return loadedFiles;
  }

  public Location getLast() {
    ensureFetched(ColumnType.LAST);
    return last;
  }

  public Collection<StoredTabletFile> getFiles() {
    ensureFetched(ColumnType.FILES);
    return files.keySet();
  }

  public Map<StoredTabletFile,DataFileValue> getFilesMap() {
    ensureFetched(ColumnType.FILES);
    return files;
  }

  public Collection<LogEntry> getLogs() {
    ensureFetched(ColumnType.LOGS);
    return logs;
  }

  public List<StoredTabletFile> getScans() {
    ensureFetched(ColumnType.SCANS);
    return scans;
  }

  public String getDirName() {
    ensureFetched(ColumnType.DIR);
    return dirName;
  }

  public MetadataTime getTime() {
    ensureFetched(ColumnType.TIME);
    return time;
  }

  public String getCloned() {
    ensureFetched(ColumnType.CLONED);
    return cloned;
  }

  public OptionalLong getFlushId() {
    ensureFetched(ColumnType.FLUSH_ID);
    return flush;
  }

  public OptionalLong getCompactId() {
    ensureFetched(ColumnType.COMPACT_ID);
    return compact;
  }

  public Double getSplitRatio() {
    ensureFetched(ColumnType.SPLIT_RATIO);
    return splitRatio;
  }

  public SortedMap<Key,Value> getKeyValues() {
    Preconditions.checkState(keyValues != null, "Requested key values when it was not saved");
    return keyValues;
  }

  @VisibleForTesting
  public static TabletMetadata convertRow(Iterator<Entry<Key,Value>> rowIter,
      EnumSet<ColumnType> fetchedColumns, boolean buildKeyValueMap) {
    Objects.requireNonNull(rowIter);

    TabletMetadata te = new TabletMetadata();
    ImmutableSortedMap.Builder<Key,Value> kvBuilder = null;
    if (buildKeyValueMap) {
      kvBuilder = ImmutableSortedMap.naturalOrder();
    }

    var filesBuilder = ImmutableMap.<StoredTabletFile,DataFileValue>builder();
    var scansBuilder = ImmutableList.<StoredTabletFile>builder();
    var logsBuilder = ImmutableList.<LogEntry>builder();
    final var loadedFilesBuilder = ImmutableMap.<TabletFile,Long>builder();
    ByteSequence row = null;

    while (rowIter.hasNext()) {
      Entry<Key,Value> kv = rowIter.next();
      Key key = kv.getKey();
      String val = kv.getValue().toString();
      String fam = key.getColumnFamilyData().toString();
      String qual = key.getColumnQualifierData().toString();

      if (buildKeyValueMap) {
        kvBuilder.put(key, kv.getValue());
      }

      if (row == null) {
        row = key.getRowData();
        KeyExtent ke = new KeyExtent(key.getRow(), (Text) null);
        te.endRow = ke.getEndRow();
        te.tableId = ke.getTableId();
      } else if (!row.equals(key.getRowData())) {
        throw new IllegalArgumentException(
            "Input contains more than one row : " + row + " " + key.getRowData());
      }

      switch (fam.toString()) {
        case TabletColumnFamily.STR_NAME:
          switch (qual) {
            case PREV_ROW_QUAL:
              te.prevEndRow = KeyExtent.decodePrevEndRow(kv.getValue());
              te.sawPrevEndRow = true;
              break;
            case OLD_PREV_ROW_QUAL:
              te.oldPrevEndRow = KeyExtent.decodePrevEndRow(kv.getValue());
              te.sawOldPrevEndRow = true;
              break;
            case SPLIT_RATIO_QUAL:
              te.splitRatio = Double.parseDouble(val);
              break;
          }
          break;
        case ServerColumnFamily.STR_NAME:
          switch (qual) {
            case DIRECTORY_QUAL:
              Preconditions.checkArgument(ServerColumnFamily.isValidDirCol(val),
                  "Saw invalid dir name {} {}", key, val);
              te.dirName = val;
              break;
            case TIME_QUAL:
              te.time = MetadataTime.parse(val);
              break;
            case FLUSH_QUAL:
              te.flush = OptionalLong.of(Long.parseLong(val));
              break;
            case COMPACT_QUAL:
              te.compact = OptionalLong.of(Long.parseLong(val));
              break;
          }
          break;
        case DataFileColumnFamily.STR_NAME:
          filesBuilder.put(new StoredTabletFile(qual), new DataFileValue(val));
          break;
        case BulkFileColumnFamily.STR_NAME:
          loadedFilesBuilder.put(new StoredTabletFile(qual),
              BulkFileColumnFamily.getBulkLoadTid(val));
          break;
        case CurrentLocationColumnFamily.STR_NAME:
          te.setLocationOnce(val, qual, LocationType.CURRENT);
          break;
        case FutureLocationColumnFamily.STR_NAME:
          te.setLocationOnce(val, qual, LocationType.FUTURE);
          break;
        case LastLocationColumnFamily.STR_NAME:
          te.last = new Location(val, qual, LocationType.LAST);
          break;
        case ScanFileColumnFamily.STR_NAME:
          scansBuilder.add(new StoredTabletFile(qual));
          break;
        case ClonedColumnFamily.STR_NAME:
          te.cloned = val;
          break;
        case LogColumnFamily.STR_NAME:
          logsBuilder.add(LogEntry.fromKeyValue(key, val));
          break;
        default:
          throw new IllegalStateException("Unexpected family " + fam);
      }
    }

    te.files = filesBuilder.build();
    te.loadedFiles = loadedFilesBuilder.build();
    te.fetchedCols = fetchedColumns;
    te.scans = scansBuilder.build();
    te.logs = logsBuilder.build();
    if (buildKeyValueMap) {
      te.keyValues = kvBuilder.build();
    }
    return te;
  }

  private void setLocationOnce(String val, String qual, LocationType lt) {
    if (location != null) {
      throw new IllegalStateException("Attempted to set second location for tableId: " + tableId
          + " endrow: " + endRow + " -- " + location + " " + qual + " " + val);
    }
    location = new Location(val, qual, lt);
  }

  static Iterable<TabletMetadata> convert(Scanner input, EnumSet<ColumnType> fetchedColumns,
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
    te.tableId = TableId.of(id);
    te.sawPrevEndRow = true;
    te.prevEndRow = prevEndRow == null ? null : new Text(prevEndRow);
    te.endRow = endRow == null ? null : new Text(endRow);
    te.fetchedCols = EnumSet.of(ColumnType.PREV_ROW);
    return te;
  }
}
