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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily.GOAL_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily.REQUESTED_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.OPID_QUAL;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.SELECTED_QUAL;
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
import java.util.SortedMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletHostingGoalUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
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
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.net.HostAndPort;

public class TabletMetadata {

  private static final Logger log = LoggerFactory.getLogger(TabletMetadata.class);

  private TableId tableId;
  private Text prevEndRow;
  private boolean sawPrevEndRow = false;
  private Text oldPrevEndRow;
  private boolean sawOldPrevEndRow = false;
  private Text endRow;
  private Location location;
  private Map<StoredTabletFile,DataFileValue> files;
  private List<StoredTabletFile> scans;
  private Map<StoredTabletFile,Long> loadedFiles;
  private SelectedFiles selectedFiles;
  private EnumSet<ColumnType> fetchedCols;
  private KeyExtent extent;
  private Location last;
  private SuspendingTServer suspend;
  private String dirName;
  private MetadataTime time;
  private String cloned;
  private SortedMap<Key,Value> keyValues;
  private OptionalLong flush = OptionalLong.empty();
  private List<LogEntry> logs;
  private OptionalLong compact = OptionalLong.empty();
  private Double splitRatio = null;
  private Map<ExternalCompactionId,ExternalCompactionMetadata> extCompactions;
  private TabletHostingGoal goal = TabletHostingGoal.ONDEMAND;
  private boolean onDemandHostingRequested = false;
  private TabletOperationId operationId;
  private boolean futureAndCurrentLocationSet = false;
  private Set<Long> compacted;

  public static TabletMetadataBuilder builder(KeyExtent extent) {
    return new TabletMetadataBuilder(extent);
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
    HOSTING_GOAL,
    HOSTING_REQUESTED,
    OPID,
    SELECTED,
    COMPACTED
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

  // ELASTICITY_TODO remove and handle in upgrade
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

  public Set<StoredTabletFile> getFiles() {
    ensureFetched(ColumnType.FILES);
    return files.keySet();
  }

  public Map<StoredTabletFile,DataFileValue> getFilesMap() {
    ensureFetched(ColumnType.FILES);
    return files;
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

  public OptionalLong getCompactId() {
    ensureFetched(ColumnType.COMPACT_ID);
    return compact;
  }

  public Double getSplitRatio() {
    ensureFetched(ColumnType.SPLIT_RATIO);
    return splitRatio;
  }

  public TabletHostingGoal getHostingGoal() {
    if (RootTable.ID.equals(getTableId()) || MetadataTable.ID.equals(getTableId())) {
      // Override the goal for the system tables
      return TabletHostingGoal.ALWAYS;
    }
    ensureFetched(ColumnType.HOSTING_GOAL);
    return goal;
  }

  public boolean getHostingRequested() {
    ensureFetched(ColumnType.HOSTING_REQUESTED);
    return onDemandHostingRequested;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("tableId", tableId)
        .append("prevEndRow", prevEndRow).append("sawPrevEndRow", sawPrevEndRow)
        .append("oldPrevEndRow", oldPrevEndRow).append("sawOldPrevEndRow", sawOldPrevEndRow)
        .append("endRow", endRow).append("location", location).append("files", files)
        .append("scans", scans).append("loadedFiles", loadedFiles)
        .append("fetchedCols", fetchedCols).append("extent", extent).append("last", last)
        .append("suspend", suspend).append("dirName", dirName).append("time", time)
        .append("cloned", cloned).append("flush", flush).append("logs", logs)
        .append("compact", compact).append("splitRatio", splitRatio)
        .append("extCompactions", extCompactions).append("goal", goal)
        .append("onDemandHostingRequested", onDemandHostingRequested)
        .append("operationId", operationId).append("selectedFiles", selectedFiles)
        .append("futureAndCurrentLocationSet", futureAndCurrentLocationSet).toString();
  }

  public SortedMap<Key,Value> getKeyValues() {
    Preconditions.checkState(keyValues != null, "Requested key values when it was not saved");
    return keyValues;
  }

  public Map<ExternalCompactionId,ExternalCompactionMetadata> getExternalCompactions() {
    ensureFetched(ColumnType.ECOMP);
    return extCompactions;
  }

  public Set<Long> getCompacted() {
    ensureFetched(ColumnType.COMPACTED);
    return compacted;
  }

  /**
   * @return the operation id if it exist, null otherwise
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

    TabletMetadata te = new TabletMetadata();
    final ImmutableSortedMap.Builder<Key,Value> kvBuilder =
        buildKeyValueMap ? ImmutableSortedMap.naturalOrder() : null;

    final var filesBuilder = ImmutableMap.<StoredTabletFile,DataFileValue>builder();
    final var scansBuilder = ImmutableList.<StoredTabletFile>builder();
    final var logsBuilder = ImmutableList.<LogEntry>builder();
    final var extCompBuilder =
        ImmutableMap.<ExternalCompactionId,ExternalCompactionMetadata>builder();
    final var loadedFilesBuilder = ImmutableMap.<StoredTabletFile,Long>builder();
    final var compactedBuilder = ImmutableSet.<Long>builder();
    ByteSequence row = null;

    while (rowIter.hasNext()) {
      final Entry<Key,Value> kv = rowIter.next();
      final Key key = kv.getKey();
      final String val = kv.getValue().toString();
      final String fam = key.getColumnFamilyData().toString();
      final String qual = key.getColumnQualifierData().toString();

      if (buildKeyValueMap) {
        kvBuilder.put(key, kv.getValue());
      }

      if (row == null) {
        row = key.getRowData();
        KeyExtent ke = KeyExtent.fromMetaRow(key.getRow());
        te.endRow = ke.endRow();
        te.tableId = ke.tableId();
      } else if (!row.equals(key.getRowData())) {
        throw new IllegalArgumentException(
            "Input contains more than one row : " + row + " " + key.getRowData());
      }

      switch (fam.toString()) {
        case TabletColumnFamily.STR_NAME:
          switch (qual) {
            case PREV_ROW_QUAL:
              te.prevEndRow = TabletColumnFamily.decodePrevEndRow(kv.getValue());
              te.sawPrevEndRow = true;
              break;
            case OLD_PREV_ROW_QUAL:
              te.oldPrevEndRow = TabletColumnFamily.decodePrevEndRow(kv.getValue());
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
                  "Saw invalid dir name %s %s", key, val);
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
            case OPID_QUAL:
              te.setOperationIdOnce(val, suppressLocationError);
              break;
            case SELECTED_QUAL:
              te.selectedFiles = SelectedFiles.from(val);
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
          te.setLocationOnce(val, qual, LocationType.CURRENT, suppressLocationError);
          break;
        case FutureLocationColumnFamily.STR_NAME:
          te.setLocationOnce(val, qual, LocationType.FUTURE, suppressLocationError);
          break;
        case LastLocationColumnFamily.STR_NAME:
          te.last = Location.last(val, qual);
          break;
        case SuspendLocationColumn.STR_NAME:
          te.suspend = SuspendingTServer.fromValue(kv.getValue());
          break;
        case ScanFileColumnFamily.STR_NAME:
          scansBuilder.add(new StoredTabletFile(qual));
          break;
        case ClonedColumnFamily.STR_NAME:
          te.cloned = val;
          break;
        case LogColumnFamily.STR_NAME:
          logsBuilder.add(LogEntry.fromMetaWalEntry(kv));
          break;
        case ExternalCompactionColumnFamily.STR_NAME:
          extCompBuilder.put(ExternalCompactionId.of(qual),
              ExternalCompactionMetadata.fromJson(val));
          break;
        case CompactedColumnFamily.STR_NAME:
          compactedBuilder.add(FateTxId.fromString(qual));
          break;
        case HostingColumnFamily.STR_NAME:
          switch (qual) {
            case GOAL_QUAL:
              te.goal = TabletHostingGoalUtil.fromValue(kv.getValue());
              break;
            case REQUESTED_QUAL:
              te.onDemandHostingRequested = true;
              break;
            default:
              throw new IllegalStateException("Unexpected family " + fam);
          }
          break;
      }
    }

    if (RootTable.ID.equals(te.tableId) || MetadataTable.ID.equals(te.tableId)) {
      // Override the goal for the system tables
      te.goal = TabletHostingGoal.ALWAYS;
    }

    te.files = filesBuilder.build();
    te.loadedFiles = loadedFilesBuilder.build();
    te.fetchedCols = fetchedColumns;
    te.scans = scansBuilder.build();
    te.logs = logsBuilder.build();
    te.extCompactions = extCompBuilder.build();
    te.compacted = compactedBuilder.build();
    if (buildKeyValueMap) {
      te.keyValues = kvBuilder.build();
    }
    return te;
  }

  /**
   * Sets a location only once.
   *
   * @param val server to set for Location object
   * @param qual session to set for Location object
   * @param lt location type to use to construct Location object
   * @param suppressError set to true to suppress an exception being thrown, else false
   * @throws IllegalStateException if an operation id or location is already set
   */
  private void setLocationOnce(String val, String qual, LocationType lt, boolean suppressError) {
    if (location != null) {
      if (!suppressError) {
        throw new IllegalStateException("Attempted to set second location for tableId: " + tableId
            + " endrow: " + endRow + " -- " + location + " " + qual + " " + val);
      }
      futureAndCurrentLocationSet = true;
    }
    location = new Location(val, qual, lt);
  }

  /**
   * Sets an operation ID only once.
   *
   * @param val operation id to set
   * @param suppressError set to true to suppress an exception being thrown, else false
   * @throws IllegalStateException if an operation id or location is already set
   */
  private void setOperationIdOnce(String val, boolean suppressError) {
    Preconditions.checkState(operationId == null);
    operationId = TabletOperationId.from(val);
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
}
