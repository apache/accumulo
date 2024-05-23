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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.OLD_PREV_ROW_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.SPLIT_RATIO_QUAL;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.MergedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.common.net.HostAndPort;

public class TabletMetadata {
  private static final Logger log = LoggerFactory.getLogger(TabletMetadata.class);

  private final TableId tableId;
  private final Text prevEndRow;
  private final boolean sawPrevEndRow;
  private final Text oldPrevEndRow;
  private final boolean sawOldPrevEndRow;
  private final Text endRow;
  private final Location location;
  private final Map<StoredTabletFile,DataFileValue> files;
  private final List<StoredTabletFile> scans;
  private final Map<StoredTabletFile,Long> loadedFiles;
  private final EnumSet<ColumnType> fetchedCols;
  private final Supplier<KeyExtent> extent;
  private final Location last;
  private final SuspendingTServer suspend;
  private final String dirName;
  private final MetadataTime time;
  private final String cloned;
  private final List<Entry<Key,Value>> keyValues;
  private final OptionalLong flush;
  private final List<LogEntry> logs;
  private final OptionalLong compact;
  private final Double splitRatio;
  private final Map<ExternalCompactionId,ExternalCompactionMetadata> extCompactions;
  private final boolean merged;

  private TabletMetadata(Builder tmBuilder) {
    this.tableId = tmBuilder.tableId;
    this.prevEndRow = tmBuilder.prevEndRow;
    this.sawPrevEndRow = tmBuilder.sawPrevEndRow;
    this.oldPrevEndRow = tmBuilder.oldPrevEndRow;
    this.sawOldPrevEndRow = tmBuilder.sawOldPrevEndRow;
    this.endRow = tmBuilder.endRow;
    this.location = tmBuilder.location;
    this.files = Objects.requireNonNull(tmBuilder.files.build());
    this.scans = Objects.requireNonNull(tmBuilder.scans.build());
    this.loadedFiles = tmBuilder.loadedFiles.build();
    this.fetchedCols = Objects.requireNonNull(tmBuilder.fetchedCols);
    this.last = tmBuilder.last;
    this.suspend = tmBuilder.suspend;
    this.dirName = tmBuilder.dirName;
    this.time = tmBuilder.time;
    this.cloned = tmBuilder.cloned;
    this.keyValues =
        Optional.ofNullable(tmBuilder.keyValues).map(ImmutableList.Builder::build).orElse(null);
    this.flush = tmBuilder.flush;
    this.logs = Objects.requireNonNull(tmBuilder.logs.build());
    this.compact = Objects.requireNonNull(tmBuilder.compact);
    this.splitRatio = tmBuilder.splitRatio;
    this.extCompactions = Objects.requireNonNull(tmBuilder.extCompactions.build());
    this.merged = tmBuilder.merged;
    this.extent =
        Suppliers.memoize(() -> new KeyExtent(getTableId(), getEndRow(), getPrevEndRow()));
  }

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
    SPLIT_RATIO,
    SUSPEND,
    ECOMP,
    MERGED
  }

  public static class Location {
    private final TServerInstance tServerInstance;
    private final LocationType lt;

    private Location(final String server, final String session, final LocationType lt) {
      this(new TServerInstance(HostAndPort.fromString(server), session), lt);
    }

    private Location(final TServerInstance tServerInstance, final LocationType lt) {
      this.tServerInstance =
          Objects.requireNonNull(tServerInstance, "tServerInstance must not be null");
      this.lt = Objects.requireNonNull(lt, "locationType must not be null");
    }

    public LocationType getType() {
      return lt;
    }

    public TServerInstance getServerInstance() {
      return tServerInstance;
    }

    public String getHostPortSession() {
      return tServerInstance.getHostPortSession();
    }

    public String getHost() {
      return tServerInstance.getHost();
    }

    public String getHostPort() {
      return tServerInstance.getHostPort();
    }

    public HostAndPort getHostAndPort() {
      return tServerInstance.getHostAndPort();
    }

    public String getSession() {
      return tServerInstance.getSession();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Location location = (Location) o;
      return Objects.equals(tServerInstance, location.tServerInstance) && lt == location.lt;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tServerInstance, lt);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(32);
      sb.append("Location [");
      sb.append("server=").append(tServerInstance);
      sb.append(", type=").append(lt);
      sb.append("]");
      return sb.toString();
    }

    public static Location last(TServerInstance instance) {
      return new Location(instance, LocationType.LAST);
    }

    public static Location last(final String server, final String session) {
      return last(new TServerInstance(HostAndPort.fromString(server), session));
    }

    public static Location current(TServerInstance instance) {
      return new Location(instance, LocationType.CURRENT);
    }

    public static Location current(final String server, final String session) {
      return current(new TServerInstance(HostAndPort.fromString(server), session));
    }

    public static Location future(TServerInstance instance) {
      return new Location(instance, LocationType.FUTURE);
    }

    public static Location future(final String server, final String session) {
      return future(new TServerInstance(HostAndPort.fromString(server), session));
    }

  }

  public TableId getTableId() {
    return tableId;
  }

  public KeyExtent getExtent() {
    return extent.get();
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

  public Map<StoredTabletFile,Long> getLoaded() {
    ensureFetched(ColumnType.LOADED);
    return loadedFiles;
  }

  public Location getLast() {
    ensureFetched(ColumnType.LAST);
    return last;
  }

  public SuspendingTServer getSuspend() {
    ensureFetched(ColumnType.SUSPEND);
    return suspend;
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

  public boolean hasMerged() {
    ensureFetched(ColumnType.MERGED);
    return merged;
  }

  public List<Entry<Key,Value>> getKeyValues() {
    Preconditions.checkState(keyValues != null, "Requested key values when it was not saved");
    return keyValues;
  }

  public TabletState getTabletState(Set<TServerInstance> liveTServers) {
    ensureFetched(ColumnType.LOCATION);
    ensureFetched(ColumnType.LAST);
    ensureFetched(ColumnType.SUSPEND);
    try {
      Location current = null;
      Location future = null;
      if (hasCurrent()) {
        current = location;
      } else {
        future = location;
      }
      // only care about the state so don't need walogs and chopped params
      // Use getExtent() when passing the extent as the private reference may not have been
      // initialized yet. This will also ensure PREV_ROW was fetched
      var tls = new TabletLocationState(getExtent(), future, current, last, suspend, null);
      return tls.getState(liveTServers);
    } catch (TabletLocationState.BadLocationStateException blse) {
      throw new IllegalArgumentException("Error creating TabletLocationState", blse);
    }
  }

  public Map<ExternalCompactionId,ExternalCompactionMetadata> getExternalCompactions() {
    ensureFetched(ColumnType.ECOMP);
    return extCompactions;
  }

  @VisibleForTesting
  public static <E extends Entry<Key,Value>> TabletMetadata convertRow(Iterator<E> rowIter,
      EnumSet<ColumnType> fetchedColumns, boolean buildKeyValueMap) {
    Objects.requireNonNull(rowIter);

    final var tmBuilder = new Builder();
    ByteSequence row = null;

    while (rowIter.hasNext()) {
      final Entry<Key,Value> kv = rowIter.next();
      final Key key = kv.getKey();
      final String val = kv.getValue().toString();
      final String fam = key.getColumnFamilyData().toString();
      final String qual = key.getColumnQualifierData().toString();

      if (buildKeyValueMap) {
        tmBuilder.keyValue(kv);
      }

      if (row == null) {
        row = key.getRowData();
        KeyExtent ke = KeyExtent.fromMetaRow(key.getRow());
        tmBuilder.endRow(ke.endRow());
        tmBuilder.table(ke.tableId());
      } else if (!row.equals(key.getRowData())) {
        throw new IllegalArgumentException(
            "Input contains more than one row : " + row + " " + key.getRowData());
      }

      switch (fam.toString()) {
        case TabletColumnFamily.STR_NAME:
          switch (qual) {
            case PREV_ROW_QUAL:
              tmBuilder.prevEndRow(TabletColumnFamily.decodePrevEndRow(kv.getValue()));
              tmBuilder.sawPrevEndRow(true);
              break;
            case OLD_PREV_ROW_QUAL:
              tmBuilder.oldPrevEndRow(TabletColumnFamily.decodePrevEndRow(kv.getValue()));
              tmBuilder.sawOldPrevEndRow(true);
              break;
            case SPLIT_RATIO_QUAL:
              tmBuilder.splitRatio(Double.parseDouble(val));
              break;
          }
          break;
        case ServerColumnFamily.STR_NAME:
          switch (qual) {
            case DIRECTORY_QUAL:
              Preconditions.checkArgument(ServerColumnFamily.isValidDirCol(val),
                  "Saw invalid dir name %s %s", key, val);
              tmBuilder.dirName(val);
              break;
            case TIME_QUAL:
              tmBuilder.time(MetadataTime.parse(val));
              break;
            case FLUSH_QUAL:
              tmBuilder.flush(Long.parseLong(val));
              break;
            case COMPACT_QUAL:
              tmBuilder.compact(Long.parseLong(val));
              break;
          }
          break;
        case DataFileColumnFamily.STR_NAME:
          tmBuilder.file(new StoredTabletFile(qual), new DataFileValue(val));
          break;
        case BulkFileColumnFamily.STR_NAME:
          tmBuilder.loadedFile(new StoredTabletFile(qual),
              BulkFileColumnFamily.getBulkLoadTid(val));
          break;
        case CurrentLocationColumnFamily.STR_NAME:
          tmBuilder.location(val, qual, LocationType.CURRENT);
          break;
        case FutureLocationColumnFamily.STR_NAME:
          tmBuilder.location(val, qual, LocationType.FUTURE);
          break;
        case LastLocationColumnFamily.STR_NAME:
          tmBuilder.last(Location.last(val, qual));
          break;
        case SuspendLocationColumn.STR_NAME:
          tmBuilder.suspend(SuspendingTServer.fromValue(kv.getValue()));
          break;
        case ScanFileColumnFamily.STR_NAME:
          tmBuilder.scan(new StoredTabletFile(qual));
          break;
        case ClonedColumnFamily.STR_NAME:
          tmBuilder.cloned(val);
          break;
        case LogColumnFamily.STR_NAME:
          tmBuilder.log(LogEntry.fromMetaWalEntry(kv));
          break;
        case ExternalCompactionColumnFamily.STR_NAME:
          tmBuilder.extCompaction(ExternalCompactionId.of(qual),
              ExternalCompactionMetadata.fromJson(val));
          break;
        case MergedColumnFamily.STR_NAME:
          tmBuilder.merged(true);
          break;
        default:
          throw new IllegalStateException("Unexpected family " + fam);
      }
    }

    return tmBuilder.build(fetchedColumns);
  }

  @VisibleForTesting
  static TabletMetadata create(String id, String prevEndRow, String endRow) {
    final var tmBuilder = new Builder();
    tmBuilder.table(TableId.of(id));
    tmBuilder.sawPrevEndRow(true);
    tmBuilder.prevEndRow(prevEndRow == null ? null : new Text(prevEndRow));
    tmBuilder.endRow(endRow == null ? null : new Text(endRow));
    return tmBuilder.build(EnumSet.of(ColumnType.PREV_ROW));
  }

  /**
   * Get the tservers that are live from ZK. Live servers will have a valid ZooLock. This method was
   * pulled from org.apache.accumulo.server.manager.LiveTServerSet
   */
  public static synchronized Set<TServerInstance> getLiveTServers(ClientContext context) {
    final Set<TServerInstance> liveServers = new HashSet<>();

    final String path = context.getZooKeeperRoot() + Constants.ZTSERVERS;

    for (String child : context.getZooCache().getChildren(path)) {
      checkServer(context, path, child).ifPresent(liveServers::add);
    }
    log.trace("Found {} live tservers at ZK path: {}", liveServers.size(), path);

    return liveServers;
  }

  /**
   * Check for tserver ZooLock at the ZK location. Return Optional containing TServerInstance if a
   * valid Zoolock exists.
   */
  private static Optional<TServerInstance> checkServer(ClientContext context, String path,
      String zPath) {
    final var lockPath = ServiceLock.path(path + "/" + zPath);
    ZooCache.ZcStat stat = new ZooCache.ZcStat();
    log.trace("Checking server at ZK path = " + lockPath);
    return ServiceLock.getLockData(context.getZooCache(), lockPath, stat)
        .map(sld -> sld.getAddress(ServiceLockData.ThriftService.TSERV))
        .map(address -> new TServerInstance(address, stat.getEphemeralOwner()));
  }

  static class Builder {
    private TableId tableId;
    private Text prevEndRow;
    private boolean sawPrevEndRow;
    private Text oldPrevEndRow;
    private boolean sawOldPrevEndRow;
    private Text endRow;
    private Location location;
    private final ImmutableMap.Builder<StoredTabletFile,DataFileValue> files =
        ImmutableMap.builder();
    private final ImmutableList.Builder<StoredTabletFile> scans = ImmutableList.builder();
    private final ImmutableMap.Builder<StoredTabletFile,Long> loadedFiles = ImmutableMap.builder();
    private EnumSet<ColumnType> fetchedCols;
    private Location last;
    private SuspendingTServer suspend;
    private String dirName;
    private MetadataTime time;
    private String cloned;
    private ImmutableList.Builder<Entry<Key,Value>> keyValues;
    private OptionalLong flush = OptionalLong.empty();
    private final ImmutableList.Builder<LogEntry> logs = ImmutableList.builder();
    private OptionalLong compact = OptionalLong.empty();
    private Double splitRatio = null;
    private final ImmutableMap.Builder<ExternalCompactionId,
        ExternalCompactionMetadata> extCompactions = ImmutableMap.builder();
    private boolean merged;

    void table(TableId tableId) {
      this.tableId = tableId;
    }

    void endRow(Text endRow) {
      this.endRow = endRow;
    }

    void prevEndRow(Text prevEndRow) {
      this.prevEndRow = prevEndRow;
    }

    void sawPrevEndRow(boolean sawPrevEndRow) {
      this.sawPrevEndRow = sawPrevEndRow;
    }

    void oldPrevEndRow(Text oldPrevEndRow) {
      this.oldPrevEndRow = oldPrevEndRow;
    }

    void sawOldPrevEndRow(boolean sawOldPrevEndRow) {
      this.sawOldPrevEndRow = sawOldPrevEndRow;
    }

    void splitRatio(Double splitRatio) {
      this.splitRatio = splitRatio;
    }

    void dirName(String dirName) {
      this.dirName = dirName;
    }

    void time(MetadataTime time) {
      this.time = time;
    }

    void flush(long flush) {
      this.flush = OptionalLong.of(flush);
    }

    void compact(long compact) {
      this.compact = OptionalLong.of(compact);
    }

    void file(StoredTabletFile stf, DataFileValue dfv) {
      this.files.put(stf, dfv);
    }

    void loadedFile(StoredTabletFile stf, Long tid) {
      this.loadedFiles.put(stf, tid);
    }

    void location(String val, String qual, LocationType lt) {
      if (location != null) {
        throw new IllegalStateException("Attempted to set second location for tableId: " + tableId
            + " endrow: " + endRow + " -- " + location + " " + qual + " " + val);
      }
      this.location = new Location(val, qual, lt);
    }

    void last(Location last) {
      this.last = last;
    }

    void suspend(SuspendingTServer suspend) {
      this.suspend = suspend;
    }

    void scan(StoredTabletFile stf) {
      this.scans.add(stf);
    }

    void cloned(String cloned) {
      this.cloned = cloned;
    }

    void log(LogEntry log) {
      this.logs.add(log);
    }

    void extCompaction(ExternalCompactionId id, ExternalCompactionMetadata metadata) {
      this.extCompactions.put(id, metadata);
    }

    void merged(boolean merged) {
      this.merged = merged;
    }

    void keyValue(Entry<Key,Value> kv) {
      if (this.keyValues == null) {
        this.keyValues = ImmutableList.builder();
      }
      this.keyValues.add(kv);
    }

    TabletMetadata build(EnumSet<ColumnType> fetchedCols) {
      this.fetchedCols = fetchedCols;
      return new TabletMetadata(this);
    }
  }
}
