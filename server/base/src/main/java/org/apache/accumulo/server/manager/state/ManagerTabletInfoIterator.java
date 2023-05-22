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
package org.apache.accumulo.server.manager.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.manager.state.ManagerTabletInfo;
import org.apache.accumulo.core.manager.state.ManagerTabletInfo.ManagementAction;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ChoppedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class ManagerTabletInfoIterator extends SkippingIterator {

  private static final Logger LOG = LoggerFactory.getLogger(ManagerTabletInfoIterator.class);

  private static final String SERVERS_OPTION = "servers";
  private static final String TABLES_OPTION = "tables";
  private static final String MERGES_OPTION = "merges";
  private static final String DEBUG_OPTION = "debug";
  private static final String MIGRATIONS_OPTION = "migrations";
  private static final String MANAGER_STATE_OPTION = "managerState";
  private static final String SHUTTING_DOWN_OPTION = "shuttingDown";

  private static void setCurrentServers(final IteratorSetting cfg,
      final Set<TServerInstance> goodServers) {
    if (goodServers != null) {
      List<String> servers = new ArrayList<>();
      for (TServerInstance server : goodServers) {
        servers.add(server.getHostPortSession());
      }
      cfg.addOption(SERVERS_OPTION, Joiner.on(",").join(servers));
    }
  }

  private static void setOnlineTables(final IteratorSetting cfg, final Set<TableId> onlineTables) {
    if (onlineTables != null) {
      cfg.addOption(TABLES_OPTION, Joiner.on(",").join(onlineTables));
    }
  }

  private static void setMerges(final IteratorSetting cfg, final Collection<MergeInfo> merges) {
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (MergeInfo info : merges) {
        KeyExtent extent = info.getExtent();
        if (extent != null && !info.getState().equals(MergeState.NONE)) {
          info.write(buffer);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String encoded =
        Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength()));
    cfg.addOption(MERGES_OPTION, encoded);
  }

  private static void setMigrations(final IteratorSetting cfg,
      final Collection<KeyExtent> migrations) {
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (KeyExtent extent : migrations) {
        extent.writeTo(buffer);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String encoded =
        Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength()));
    cfg.addOption(MIGRATIONS_OPTION, encoded);
  }

  private static void setManagerState(final IteratorSetting cfg, final ManagerState state) {
    cfg.addOption(MANAGER_STATE_OPTION, state.toString());
  }

  private static void setShuttingDown(final IteratorSetting cfg,
      final Set<TServerInstance> servers) {
    if (servers != null) {
      cfg.addOption(SHUTTING_DOWN_OPTION, Joiner.on(",").join(servers));
    }
  }

  private static Set<KeyExtent> parseMigrations(final String migrations) {
    if (migrations == null) {
      return Collections.emptySet();
    }
    try {
      Set<KeyExtent> result = new HashSet<>();
      DataInputBuffer buffer = new DataInputBuffer();
      byte[] data = Base64.getDecoder().decode(migrations);
      buffer.reset(data, data.length);
      while (buffer.available() > 0) {
        result.add(KeyExtent.readFrom(buffer));
      }
      return result;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static Set<TableId> parseTableIDs(final String tableIDs) {
    if (tableIDs == null) {
      return null;
    }
    Set<TableId> result = new HashSet<>();
    for (String tableID : tableIDs.split(",")) {
      result.add(TableId.of(tableID));
    }
    return result;
  }

  private static Set<TServerInstance> parseServers(final String servers) {
    if (servers == null) {
      return null;
    }
    // parse "host:port[INSTANCE]"
    Set<TServerInstance> result = new HashSet<>();
    if (!servers.isEmpty()) {
      for (String part : servers.split(",")) {
        String[] parts = part.split("\\[", 2);
        String hostport = parts[0];
        String instance = parts[1];
        if (instance != null && instance.endsWith("]")) {
          instance = instance.substring(0, instance.length() - 1);
        }
        result.add(new TServerInstance(AddressUtil.parseAddress(hostport, false), instance));
      }
    }
    return result;
  }

  private static Map<TableId,MergeInfo> parseMerges(final String merges) {
    if (merges == null) {
      return null;
    }
    try {
      Map<TableId,MergeInfo> result = new HashMap<>();
      DataInputBuffer buffer = new DataInputBuffer();
      byte[] data = Base64.getDecoder().decode(merges);
      buffer.reset(data, data.length);
      while (buffer.available() > 0) {
        MergeInfo mergeInfo = new MergeInfo();
        mergeInfo.readFields(buffer);
        result.put(mergeInfo.extent.tableId(), mergeInfo);
      }
      return result;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static boolean shouldReturnDueToSplit(final TabletMetadata tm,
      final long splitThreshold) {
    return tm.getFilesMap().values().stream().map(DataFileValue::getSize)
        .collect(Collectors.summarizingLong(Long::longValue)).getSum() > splitThreshold;
  }

  private static boolean shouldReturnDueToLocation(final TabletMetadata tm,
      final Set<TableId> onlineTables, final Set<TServerInstance> current, final boolean debug) {
    // is the table supposed to be online or offline?
    final boolean shouldBeOnline = onlineTables.contains(tm.getTableId());

    if (debug) {
      LOG.debug("{} is {}. Table is {}line. Tablet hosting goal is {}, hostingRequested: {}",
          tm.getExtent(), getLocationState(current, tm), (shouldBeOnline ? "on" : "off"),
          tm.getHostingGoal(), tm.getHostingRequested());
    }
    switch (getLocationState(current, tm)) {
      case ASSIGNED:
        // we always want data about assigned tablets
        return true;
      case HOSTED:
        if (!shouldBeOnline || tm.getHostingGoal() == TabletHostingGoal.NEVER
            || (tm.getHostingGoal() == TabletHostingGoal.ONDEMAND && !tm.getHostingRequested())) {
          return true;
        }
        break;
      case ASSIGNED_TO_DEAD_SERVER:
        return true;
      case SUSPENDED:
      case UNASSIGNED:
        if (shouldBeOnline && (tm.getHostingGoal() == TabletHostingGoal.ALWAYS
            || (tm.getHostingGoal() == TabletHostingGoal.ONDEMAND && tm.getHostingRequested()))) {
          return true;
        }
        break;
      default:
        throw new AssertionError(
            "Inconceivable! The tablet is an unrecognized state: " + getLocationState(current, tm));
    }
    return false;
  }

  private static TabletState getLocationState(final Set<TServerInstance> liveServers,
      final TabletMetadata tm) {
    Location loc = tm.getLocation();

    if (loc == null || loc.getType() == null || loc.getServerInstance() == null) {
      return TabletState.UNASSIGNED;
    }

    if (loc.getType().equals(LocationType.FUTURE)) {
      return liveServers.contains(loc.getServerInstance()) ? TabletState.ASSIGNED
          : TabletState.ASSIGNED_TO_DEAD_SERVER;
    } else if (loc.getType().equals(LocationType.CURRENT)) {
      return liveServers.contains(loc.getServerInstance()) ? TabletState.HOSTED
          : TabletState.ASSIGNED_TO_DEAD_SERVER;
    } else if (tm.getSuspend() != null) {
      return TabletState.SUSPENDED;
    } else {
      return TabletState.UNASSIGNED;
    }
  }

  public static void configureScanner(final ScannerBase scanner, final CurrentState state) {
    TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(FutureLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(LastLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily());
    scanner.fetchColumnFamily(LogColumnFamily.NAME);
    scanner.fetchColumnFamily(ChoppedColumnFamily.NAME);
    scanner.fetchColumnFamily(HostingColumnFamily.NAME);
    scanner.addScanIterator(new IteratorSetting(1000, "wholeRows", WholeRowIterator.class));
    IteratorSetting tabletChange =
        new IteratorSetting(1001, "ManagerTabletInfoIterator", ManagerTabletInfoIterator.class);
    if (state != null) {
      ManagerTabletInfoIterator.setCurrentServers(tabletChange, state.onlineTabletServers());
      ManagerTabletInfoIterator.setOnlineTables(tabletChange, state.onlineTables());
      ManagerTabletInfoIterator.setMerges(tabletChange, state.merges());
      ManagerTabletInfoIterator.setMigrations(tabletChange, state.migrationsSnapshot());
      ManagerTabletInfoIterator.setManagerState(tabletChange, state.getManagerState());
      ManagerTabletInfoIterator.setShuttingDown(tabletChange, state.shutdownServers());
    }
    scanner.addScanIterator(tabletChange);
  }

  public static ManagerTabletInfo decode(Entry<Key,Value> e) throws IOException {
    return new ManagerTabletInfo(e.getKey(), e.getValue());
  }

  private Set<TServerInstance> current;
  private Set<TableId> onlineTables;
  private Map<TableId,MergeInfo> merges;
  private boolean debug = false;
  private Set<KeyExtent> migrations;
  private ManagerState managerState = ManagerState.NORMAL;
  private IteratorEnvironment env;
  private Key topKey = null;
  private Value topValue = null;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.env = env;
    current = parseServers(options.get(SERVERS_OPTION));
    onlineTables = parseTableIDs(options.get(TABLES_OPTION));
    merges = parseMerges(options.get(MERGES_OPTION));
    debug = options.containsKey(DEBUG_OPTION);
    migrations = parseMigrations(options.get(MIGRATIONS_OPTION));
    try {
      managerState = ManagerState.valueOf(options.get(MANAGER_STATE_OPTION));
    } catch (Exception ex) {
      if (options.get(MANAGER_STATE_OPTION) != null) {
        LOG.error("Unable to decode managerState {}", options.get(MANAGER_STATE_OPTION));
      }
    }
    Set<TServerInstance> shuttingDown = parseServers(options.get(SHUTTING_DOWN_OPTION));
    if (current != null && shuttingDown != null) {
      current.removeAll(shuttingDown);
    }
  }

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public boolean hasTop() {
    return topKey != null && topValue != null;
  }

  @Override
  public void next() throws IOException {
    topKey = null;
    topValue = null;
    super.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    topKey = null;
    topValue = null;
    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  protected void consume() throws IOException {

    final Set<ManagementAction> reasonsToReturnThisTablet = new HashSet<>();
    while (getSource().hasTop()) {
      final Key k = getSource().getTopKey();
      final Value v = getSource().getTopValue();
      final SortedMap<Key,Value> decodedRow = WholeRowIterator.decodeRow(k, v);
      final TabletMetadata tm = TabletMetadata.convertRow(decodedRow.entrySet().iterator(),
          ManagerTabletInfo.CONFIGURED_COLUMNS, false, true);

      LOG.debug("Evaluating extent: {}", tm);
      if (sendTabletToManager(tm, reasonsToReturnThisTablet)) {
        // If we simply returned here, then the client would get the encoded K,V
        // from the WholeRowIterator. However, it would not know the reason(s) why
        // it was returned. Insert a K,V pair to represent the reasons. The client
        // can pull this K,V pair from the results by looking at the colf.
        ManagerTabletInfo.addActions(decodedRow, reasonsToReturnThisTablet);
        topKey = decodedRow.firstKey();
        topValue = WholeRowIterator.encodeRow(new ArrayList<>(decodedRow.keySet()),
            new ArrayList<>(decodedRow.values()));
        LOG.debug("Returning extent with reasons: {}", reasonsToReturnThisTablet);
        return;
      }

      LOG.debug("No reason to return this extent, continuing");
      getSource().next();
    }
  }

  /**
   * Evaluates whether or not this Tablet should be returned so that it can be acted upon by the
   * Manager
   */
  private boolean sendTabletToManager(final TabletMetadata tm,
      final Set<ManagementAction> reasonsToReturnThisTablet) {

    reasonsToReturnThisTablet.clear();

    if (onlineTables == null || current == null || managerState != ManagerState.NORMAL
        || tm.isFutureAndCurrentLocationSet()) {
      // no need to check everything, we are in a known state where we want to return everything.
      reasonsToReturnThisTablet.add(ManagementAction.BAD_STATE);
      return true;
    }

    // we always want data about merges
    final MergeInfo merge = merges.get(tm.getTableId());
    if (merge != null) {
      // could make this smarter by only returning if the tablet is involved in the merge
      reasonsToReturnThisTablet.add(ManagementAction.IS_MERGING);
    }

    // always return the information for migrating tablets
    if (migrations.contains(tm.getExtent())) {
      reasonsToReturnThisTablet.add(ManagementAction.IS_MIGRATING);
    }

    if (shouldReturnDueToLocation(tm, onlineTables, current, debug)) {
      reasonsToReturnThisTablet.add(ManagementAction.NEEDS_LOCATION_UPDATE);
    }

    final long splitThreshold =
        ConfigurationTypeHelper.getFixedMemoryAsBytes(this.env.getPluginEnv()
            .getConfiguration(tm.getTableId()).get(Property.TABLE_SPLIT_THRESHOLD.getKey()));
    if (shouldReturnDueToSplit(tm, splitThreshold)) {
      reasonsToReturnThisTablet.add(ManagementAction.NEEDS_SPLITTING);
    }

    // TODO: Add compaction logic

    if (!reasonsToReturnThisTablet.isEmpty()) {
      return true;
    }
    return false;
  }
}
