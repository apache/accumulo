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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_NONCE_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.OPID_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.SELECTED_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.AVAILABILITY_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.REQUESTED_QUAL;

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
import java.util.SortedMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletAvailabilityUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CompactedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.MergedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SplitColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.UserCompactionRequestedColumnFamily;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.net.HostAndPort;

public class TabletMetadata {

  private static final Logger log = LoggerFactory.getLogger(TabletMetadata.class);

  private final TableId tableId;
  private final Text prevEndRow;
  private final boolean sawPrevEndRow;
  private final Text endRow;
  private final Location location;
  private final Map<StoredTabletFile,DataFileValue> files;
  private final List<StoredTabletFile> scans;
  private final Map<StoredTabletFile,FateId> loadedFiles;
  private final SelectedFiles selectedFiles;
  private final EnumSet<ColumnType> fetchedCols;
  private final Supplier<KeyExtent> extent;
  private final Location last;
  private final SuspendingTServer suspend;
  private final String dirName;
  private final MetadataTime time;
  private final String cloned;
  private final SortedMap<Key,Value> keyValues;
  private final OptionalLong flush;
  private final OptionalLong flushNonce;
  private final List<LogEntry> logs;
  private final Map<ExternalCompactionId,CompactionMetadata> extCompactions;
  private final boolean merged;
  private final TabletAvailability availability;
  private final boolean onDemandHostingRequested;
  private final TabletOperationId operationId;
  private final boolean futureAndCurrentLocationSet;
  private final Set<FateId> compacted;
  private final Set<FateId> userCompactionsRequested;
  private final UnSplittableMetadata unSplittableMetadata;
  private final Supplier<Long> fileSize;

  private TabletMetadata(Builder tmBuilder) {
    this.tableId = tmBuilder.tableId;
    this.prevEndRow = tmBuilder.prevEndRow;
    this.sawPrevEndRow = tmBuilder.sawPrevEndRow;
    this.endRow = tmBuilder.endRow;
    this.location = tmBuilder.location;
    this.files = Objects.requireNonNull(tmBuilder.files.build());
    this.scans = Objects.requireNonNull(tmBuilder.scans.build());
    this.loadedFiles = tmBuilder.loadedFiles.build();
    this.selectedFiles = tmBuilder.selectedFiles;
    this.fetchedCols = Objects.requireNonNull(tmBuilder.fetchedCols);
    this.last = tmBuilder.last;
    this.suspend = tmBuilder.suspend;
    this.dirName = tmBuilder.dirName;
    this.time = tmBuilder.time;
    this.cloned = tmBuilder.cloned;
    this.keyValues = Optional.ofNullable(tmBuilder.keyValues).map(ImmutableSortedMap.Builder::build)
        .orElse(null);
    this.flush = tmBuilder.flush;
    this.flushNonce = tmBuilder.flushNonce;
    this.logs = Objects.requireNonNull(tmBuilder.logs.build());
    this.extCompactions = Objects.requireNonNull(tmBuilder.extCompactions.build());
    this.merged = tmBuilder.merged;
    this.availability = Objects.requireNonNull(tmBuilder.availability);
    this.onDemandHostingRequested = tmBuilder.onDemandHostingRequested;
    this.operationId = tmBuilder.operationId;
    this.futureAndCurrentLocationSet = tmBuilder.futureAndCurrentLocationSet;
    this.compacted = tmBuilder.compacted.build();
    this.userCompactionsRequested = tmBuilder.userCompactionsRequested.build();
    this.unSplittableMetadata = tmBuilder.unSplittableMetadata;
    this.fileSize = Suppliers.memoize(() -> {
      // This code was using a java stream. While profiling SplitMillionIT, the stream was showing
      // up as hot when scanning 1 million tablets. Converted to a for loop to improve performance.
      long sum = 0;
      for (var dfv : files.values()) {
        sum += dfv.getSize();
      }
      return sum;
    });
    this.extent =
        Suppliers.memoize(() -> new KeyExtent(getTableId(), getEndRow(), getPrevEndRow()));
  }

  public static TabletMetadataBuilder builder(KeyExtent extent) {
    return new TabletMetadataBuilder(extent);
  }

  public enum LocationType {
    CURRENT, FUTURE, LAST
  }

  public enum ColumnType {
    LOCATION,
    PREV_ROW,
    FILES,
    LAST,
    LOADED,
    SCANS,
    DIR,
    TIME,
    CLONED,
    FLUSH_ID,
    FLUSH_NONCE,
    LOGS,
    SUSPEND,
    ECOMP,
    MERGED,
    AVAILABILITY,
    HOSTING_REQUESTED,
    OPID,
    SELECTED,
    COMPACTED,
    USER_COMPACTION_REQUESTED,
    UNSPLITTABLE
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

  public Map<StoredTabletFile,FateId> getLoaded() {
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

  public Set<StoredTabletFile> getFiles() {
    ensureFetched(ColumnType.FILES);
    return files.keySet();
  }

  public Map<StoredTabletFile,DataFileValue> getFilesMap() {
    ensureFetched(ColumnType.FILES);
    return files;
  }

  /**
   * @return the sum of the tablets files sizes
   */
  public long getFileSize() {
    ensureFetched(ColumnType.FILES);
    return fileSize.get();
  }

  public SelectedFiles getSelectedFiles() {
    ensureFetched(ColumnType.SELECTED);
    return selectedFiles;
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

  public OptionalLong getFlushNonce() {
    ensureFetched(ColumnType.FLUSH_NONCE);
    return flushNonce;
  }

  public boolean hasMerged() {
    ensureFetched(ColumnType.MERGED);
    return merged;
  }

  public Set<FateId> getUserCompactionsRequested() {
    ensureFetched(ColumnType.USER_COMPACTION_REQUESTED);
    return userCompactionsRequested;
  }

  public TabletAvailability getTabletAvailability() {
    if (AccumuloTable.ROOT.tableId().equals(getTableId())
        || AccumuloTable.METADATA.tableId().equals(getTableId())) {
      // Override the availability for the system tables
      return TabletAvailability.HOSTED;
    }
    ensureFetched(ColumnType.AVAILABILITY);
    return availability;
  }

  public boolean getHostingRequested() {
    ensureFetched(ColumnType.HOSTING_REQUESTED);
    return onDemandHostingRequested;
  }

  public UnSplittableMetadata getUnSplittable() {
    ensureFetched(ColumnType.UNSPLITTABLE);
    return unSplittableMetadata;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("tableId", tableId)
        .append("prevEndRow", prevEndRow).append("sawPrevEndRow", sawPrevEndRow)
        .append("endRow", endRow).append("location", location).append("files", files)
        .append("scans", scans).append("loadedFiles", loadedFiles)
        .append("fetchedCols", fetchedCols).append("extent", extent).append("last", last)
        .append("suspend", suspend).append("dirName", dirName).append("time", time)
        .append("cloned", cloned).append("flush", flush).append("logs", logs)
        .append("extCompactions", extCompactions).append("availability", availability)
        .append("onDemandHostingRequested", onDemandHostingRequested)
        .append("operationId", operationId).append("selectedFiles", selectedFiles)
        .append("futureAndCurrentLocationSet", futureAndCurrentLocationSet)
        .append("userCompactionsRequested", userCompactionsRequested)
        .append("unSplittableMetadata", unSplittableMetadata).toString();
  }

  public SortedMap<Key,Value> getKeyValues() {
    Preconditions.checkState(keyValues != null, "Requested key values when it was not saved");
    return keyValues;
  }

  public Map<ExternalCompactionId,CompactionMetadata> getExternalCompactions() {
    ensureFetched(ColumnType.ECOMP);
    return extCompactions;
  }

  public Set<FateId> getCompacted() {
    ensureFetched(ColumnType.COMPACTED);
    return compacted;
  }

  /**
   * @return the operation id if it exists, null otherwise
   * @see MetadataSchema.TabletsSection.ServerColumnFamily#OPID_COLUMN
   */
  public TabletOperationId getOperationId() {
    ensureFetched(ColumnType.OPID);
    return operationId;
  }

  public boolean isFutureAndCurrentLocationSet() {
    return futureAndCurrentLocationSet;
  }

  @VisibleForTesting
  public static <E extends Entry<Key,Value>> TabletMetadata convertRow(Iterator<E> rowIter,
      EnumSet<ColumnType> fetchedColumns, boolean buildKeyValueMap, boolean suppressLocationError) {
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
        tmBuilder.keyValue(key, kv.getValue());
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

      switch (fam) {
        case TabletColumnFamily.STR_NAME:
          switch (qual) {
            case PREV_ROW_QUAL:
              tmBuilder.prevEndRow(TabletColumnFamily.decodePrevEndRow(kv.getValue()));
              tmBuilder.sawPrevEndRow(true);
              break;
            case AVAILABILITY_QUAL:
              tmBuilder.availability(TabletAvailabilityUtil.fromValue(kv.getValue()));
              break;
            case REQUESTED_QUAL:
              tmBuilder.onDemandHostingRequested(true);
              break;
            default:
              throw new IllegalStateException("Unexpected TabletColumnFamily qualifier: " + qual);
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
            case FLUSH_NONCE_QUAL:
              tmBuilder.flushNonce(Long.parseUnsignedLong(val, 16));
              break;
            case OPID_QUAL:
              tmBuilder.operationId(val);
              break;
            case SELECTED_QUAL:
              tmBuilder.selectedFiles(SelectedFiles.from(val));
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
          tmBuilder.location(val, qual, LocationType.CURRENT, suppressLocationError);
          break;
        case FutureLocationColumnFamily.STR_NAME:
          tmBuilder.location(val, qual, LocationType.FUTURE, suppressLocationError);
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
          tmBuilder.extCompaction(ExternalCompactionId.of(qual), CompactionMetadata.fromJson(val));
          break;
        case MergedColumnFamily.STR_NAME:
          tmBuilder.merged(true);
          break;
        case CompactedColumnFamily.STR_NAME:
          tmBuilder.compacted(FateId.from(qual));
          break;
        case UserCompactionRequestedColumnFamily.STR_NAME:
          tmBuilder.userCompactionsRequested(FateId.from(qual));
          break;
        case SplitColumnFamily.STR_NAME:
          if (qual.equals(SplitColumnFamily.UNSPLITTABLE_QUAL)) {
            tmBuilder.unSplittableMetadata(UnSplittableMetadata.toUnSplittable(val));
          } else {
            throw new IllegalStateException("Unexpected SplitColumnFamily qualifier: " + qual);
          }
          break;
        default:
          throw new IllegalStateException("Unexpected family " + fam);

      }
    }

    if (AccumuloTable.ROOT.tableId().equals(tmBuilder.tableId)
        || AccumuloTable.METADATA.tableId().equals(tmBuilder.tableId)) {
      // Override the availability for the system tables
      tmBuilder.availability(TabletAvailability.HOSTED);
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
    private Text endRow;
    private Location location;
    private final ImmutableMap.Builder<StoredTabletFile,DataFileValue> files =
        ImmutableMap.builder();
    private final ImmutableList.Builder<StoredTabletFile> scans = ImmutableList.builder();
    private final ImmutableMap.Builder<StoredTabletFile,FateId> loadedFiles =
        ImmutableMap.builder();
    private SelectedFiles selectedFiles;
    private EnumSet<ColumnType> fetchedCols;
    private Location last;
    private SuspendingTServer suspend;
    private String dirName;
    private MetadataTime time;
    private String cloned;
    private ImmutableSortedMap.Builder<Key,Value> keyValues;
    private OptionalLong flush = OptionalLong.empty();
    private OptionalLong flushNonce = OptionalLong.empty();
    private final ImmutableList.Builder<LogEntry> logs = ImmutableList.builder();
    private final ImmutableMap.Builder<ExternalCompactionId,CompactionMetadata> extCompactions =
        ImmutableMap.builder();
    private boolean merged;
    private TabletAvailability availability = TabletAvailability.ONDEMAND;
    private boolean onDemandHostingRequested;
    private TabletOperationId operationId;
    private boolean futureAndCurrentLocationSet;
    private final ImmutableSet.Builder<FateId> compacted = ImmutableSet.builder();
    private final ImmutableSet.Builder<FateId> userCompactionsRequested = ImmutableSet.builder();
    private UnSplittableMetadata unSplittableMetadata;
    // private Supplier<Long> fileSize;

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

    void dirName(String dirName) {
      this.dirName = dirName;
    }

    void time(MetadataTime time) {
      this.time = time;
    }

    void flush(long flush) {
      this.flush = OptionalLong.of(flush);
    }

    void flushNonce(long flushNonce) {
      this.flushNonce = OptionalLong.of(flushNonce);
    }

    void file(StoredTabletFile stf, DataFileValue dfv) {
      this.files.put(stf, dfv);
    }

    void loadedFile(StoredTabletFile stf, FateId fateId) {
      this.loadedFiles.put(stf, fateId);
    }

    void selectedFiles(SelectedFiles selectedFiles) {
      this.selectedFiles = selectedFiles;
    }

    void location(String val, String qual, LocationType lt, boolean suppressError) {
      if (location != null) {
        if (!suppressError) {
          throw new IllegalStateException("Attempted to set second location for tableId: " + tableId
              + " endrow: " + endRow + " -- " + location + " " + qual + " " + val);
        }
        futureAndCurrentLocationSet = true;
      }
      location = new Location(val, qual, lt);
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

    void extCompaction(ExternalCompactionId id, CompactionMetadata metadata) {
      this.extCompactions.put(id, metadata);
    }

    void merged(boolean merged) {
      this.merged = merged;
    }

    void availability(TabletAvailability availability) {
      this.availability = availability;
    }

    void onDemandHostingRequested(boolean onDemandHostingRequested) {
      this.onDemandHostingRequested = onDemandHostingRequested;
    }

    void operationId(String val) {
      Preconditions.checkState(operationId == null);
      operationId = TabletOperationId.from(val);
    }

    void compacted(FateId compacted) {
      this.compacted.add(compacted);
    }

    void userCompactionsRequested(FateId userCompactionRequested) {
      this.userCompactionsRequested.add(userCompactionRequested);
    }

    void unSplittableMetadata(UnSplittableMetadata unSplittableMetadata) {
      this.unSplittableMetadata = unSplittableMetadata;
    }

    void keyValue(Key key, Value value) {
      if (this.keyValues == null) {
        this.keyValues = ImmutableSortedMap.naturalOrder();
      }
      this.keyValues.put(key, value);
    }

    TabletMetadata build(EnumSet<ColumnType> fetchedCols) {
      this.fetchedCols = fetchedCols;
      return new TabletMetadata(this);
    }
  }
}
