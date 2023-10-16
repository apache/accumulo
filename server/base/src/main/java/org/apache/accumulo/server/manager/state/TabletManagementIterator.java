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

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.HostingColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.balancer.SimpleLoadBalancer;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.server.compaction.CompactionJobGenerator;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;
import org.apache.accumulo.server.manager.balancer.BalancerEnvironmentImpl;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.gson.reflect.TypeToken;

/**
 * Iterator used by the TabletGroupWatcher threads in the Manager. This iterator returns
 * TabletManagement objects for each Tablet that needs some type of action performed on it by the
 * Manager.
 */
public class TabletManagementIterator extends SkippingIterator {
  private static final Logger LOG = LoggerFactory.getLogger(TabletManagementIterator.class);

  private static final String SERVERS_OPTION = "servers";
  private static final String TABLES_OPTION = "tables";
  private static final String MERGES_OPTION = "merges";
  private static final String MIGRATIONS_OPTION = "migrations";
  private static final String MANAGER_STATE_OPTION = "managerState";
  private static final String SHUTTING_DOWN_OPTION = "shuttingDown";
  private static final String RESOURCE_GROUPS = "resourceGroups";
  private static final String TSERVER_GROUP_PREFIX = "serverGroups_";
  private static final String COMPACTION_HINTS_OPTIONS = "compactionHints";
  private CompactionJobGenerator compactionGenerator;
  private TabletBalancer balancer;

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

  private static void setCompactionHints(final IteratorSetting cfg,
      Map<Long,Map<String,String>> allHints) {
    cfg.addOption(COMPACTION_HINTS_OPTIONS, GSON.get().toJson(allHints));
  }

  private static void setTServerResourceGroups(final IteratorSetting cfg,
      Map<String,Set<TServerInstance>> tServerResourceGroups) {
    if (tServerResourceGroups == null) {
      return;
    }
    cfg.addOption(RESOURCE_GROUPS, Joiner.on(",").join(tServerResourceGroups.keySet()));
    for (Entry<String,Set<TServerInstance>> entry : tServerResourceGroups.entrySet()) {
      cfg.addOption(TSERVER_GROUP_PREFIX + entry.getKey(), Joiner.on(",").join(entry.getValue()));
    }
  }

  private static Map<TabletServerId,String> parseTServerResourceGroups(Map<String,String> options) {
    Map<TabletServerId,String> resourceGroups = new HashMap<>();
    String groups = options.get(RESOURCE_GROUPS);
    if (groups != null) {
      for (String groupName : groups.split(",")) {
        String groupServers = options.get(TSERVER_GROUP_PREFIX + groupName);
        if (groupServers != null) {
          Set<TServerInstance> servers = parseServers(groupServers);
          servers.forEach(server -> resourceGroups.put(new TabletServerIdImpl(server), groupName));
        }
      }
    }
    return resourceGroups;
  }

  private static Set<KeyExtent> parseMigrations(final String migrations) {
    Set<KeyExtent> result = new HashSet<>();
    if (migrations != null) {
      try {
        DataInputBuffer buffer = new DataInputBuffer();
        byte[] data = Base64.getDecoder().decode(migrations);
        buffer.reset(data, data.length);
        while (buffer.available() > 0) {
          result.add(KeyExtent.readFrom(buffer));
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return result;
  }

  private static Set<TableId> parseTableIDs(final String tableIDs) {
    Set<TableId> result = new HashSet<>();
    if (tableIDs != null) {
      for (String tableID : tableIDs.split(",")) {
        result.add(TableId.of(tableID));
      }
    }
    return result;
  }

  private static Set<TServerInstance> parseServers(final String servers) {
    Set<TServerInstance> result = new HashSet<>();
    if (servers != null) {
      // parse "host:port[INSTANCE]"
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
    }
    return result;
  }

  private static Map<TableId,MergeInfo> parseMerges(final String merges) {
    Map<TableId,MergeInfo> result = new HashMap<>();
    if (merges != null) {
      try {
        DataInputBuffer buffer = new DataInputBuffer();
        byte[] data = Base64.getDecoder().decode(merges);
        buffer.reset(data, data.length);
        while (buffer.available() > 0) {
          MergeInfo mergeInfo = new MergeInfo();
          mergeInfo.readFields(buffer);
          result.put(mergeInfo.extent.tableId(), mergeInfo);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return result;
  }

  private static Map<Long,Map<String,String>> parseCompactionHints(String json) {
    if (json == null) {
      return Map.of();
    }
    Type tt = new TypeToken<Map<Long,Map<String,String>>>() {}.getType();
    return GSON.get().fromJson(json, tt);
  }

  private static boolean shouldReturnDueToSplit(final TabletMetadata tm,
      final long splitThreshold) {
    final long sumOfFileSizes =
        tm.getFilesMap().values().stream().mapToLong(DataFileValue::getSize).sum();
    final boolean shouldSplit = sumOfFileSizes > splitThreshold;
    LOG.trace("{} should split? sum: {}, threshold: {}, result: {}", tm.getExtent(), sumOfFileSizes,
        splitThreshold, shouldSplit);
    return shouldSplit;
  }

  private boolean shouldReturnDueToLocation(final TabletMetadata tm,
      final Set<TableId> onlineTables, final Set<TServerInstance> current) {

    if (migrations.contains(tm.getExtent())) {
      return true;
    }

    // is the table supposed to be online or offline?
    final boolean shouldBeOnline =
        onlineTables.contains(tm.getTableId()) && tm.getOperationId() == null;

    TabletState state = TabletState.compute(tm, current, balancer, tserverResourceGroups);
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "{} is {}. Table is {}line. Tablet hosting goal is {}, hostingRequested: {}, opId: {}",
          tm.getExtent(), state, (shouldBeOnline ? "on" : "off"), tm.getHostingGoal(),
          tm.getHostingRequested(), tm.getOperationId());
    }
    switch (state) {
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
      case NEEDS_REASSIGNMENT:
        return true;
      case SUSPENDED:
      case UNASSIGNED:
        if (shouldBeOnline && (tm.getHostingGoal() == TabletHostingGoal.ALWAYS
            || (tm.getHostingGoal() == TabletHostingGoal.ONDEMAND && tm.getHostingRequested()))) {
          return true;
        }
        break;
      default:
        throw new AssertionError("Inconceivable! The tablet is an unrecognized state: " + state);
    }
    return false;
  }

  public static void configureScanner(final ScannerBase scanner, final CurrentState state) {
    // TODO so many columns are being fetch it may not make sense to fetch columns
    TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
    ServerColumnFamily.SELECTED_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(CurrentLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(FutureLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(LastLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily());
    scanner.fetchColumnFamily(LogColumnFamily.NAME);
    scanner.fetchColumnFamily(HostingColumnFamily.NAME);
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    scanner.fetchColumnFamily(ExternalCompactionColumnFamily.NAME);
    ServerColumnFamily.OPID_COLUMN.fetch(scanner);
    scanner.addScanIterator(new IteratorSetting(1000, "wholeRows", WholeRowIterator.class));
    IteratorSetting tabletChange =
        new IteratorSetting(1001, "ManagerTabletInfoIterator", TabletManagementIterator.class);
    if (state != null) {
      TabletManagementIterator.setCurrentServers(tabletChange, state.onlineTabletServers());
      TabletManagementIterator.setOnlineTables(tabletChange, state.onlineTables());
      TabletManagementIterator.setMerges(tabletChange, state.merges());
      TabletManagementIterator.setMigrations(tabletChange, state.migrationsSnapshot());
      TabletManagementIterator.setManagerState(tabletChange, state.getManagerState());
      TabletManagementIterator.setShuttingDown(tabletChange, state.shutdownServers());
      TabletManagementIterator.setTServerResourceGroups(tabletChange,
          state.tServerResourceGroups());
      setCompactionHints(tabletChange, state.getCompactionHints());
    }
    scanner.addScanIterator(tabletChange);
  }

  public static TabletManagement decode(Entry<Key,Value> e) throws IOException {
    return new TabletManagement(e.getKey(), e.getValue());
  }

  private final Set<TServerInstance> current = new HashSet<>();
  private final Set<TableId> onlineTables = new HashSet<>();
  private final Map<TabletServerId,String> tserverResourceGroups = new HashMap<>();
  private final Map<TableId,MergeInfo> merges = new HashMap<>();
  private final Set<KeyExtent> migrations = new HashSet<>();
  private ManagerState managerState = ManagerState.NORMAL;
  private IteratorEnvironment env;
  private Key topKey = null;
  private Value topValue = null;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.env = env;
    current.addAll(parseServers(options.get(SERVERS_OPTION)));
    onlineTables.addAll(parseTableIDs(options.get(TABLES_OPTION)));
    tserverResourceGroups.putAll(parseTServerResourceGroups(options));
    merges.putAll(parseMerges(options.get(MERGES_OPTION)));
    migrations.addAll(parseMigrations(options.get(MIGRATIONS_OPTION)));
    String managerStateOptionValue = options.get(MANAGER_STATE_OPTION);
    try {
      managerState = ManagerState.valueOf(managerStateOptionValue);
    } catch (RuntimeException ex) {
      if (managerStateOptionValue != null) {
        LOG.error("Unable to decode managerState {}", managerStateOptionValue);
      }
    }
    Set<TServerInstance> shuttingDown = parseServers(options.get(SHUTTING_DOWN_OPTION));
    if (shuttingDown != null) {
      current.removeAll(shuttingDown);
    }
    compactionGenerator = new CompactionJobGenerator(env.getPluginEnv(),
        parseCompactionHints(options.get(COMPACTION_HINTS_OPTIONS)));
    final AccumuloConfiguration conf = new ConfigurationCopy(env.getPluginEnv().getConfiguration());
    BalancerEnvironmentImpl benv =
        new BalancerEnvironmentImpl(((TabletIteratorEnvironment) env).getServerContext());
    balancer = Property.createInstanceFromPropertyName(conf, Property.MANAGER_TABLET_BALANCER,
        TabletBalancer.class, new SimpleLoadBalancer());
    balancer.init(benv);
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
  protected void consume() throws IOException {
    topKey = null;
    topValue = null;

    final Set<ManagementAction> actions = new HashSet<>();
    while (getSource().hasTop()) {
      final Key k = getSource().getTopKey();
      final Value v = getSource().getTopValue();
      final SortedMap<Key,Value> decodedRow = WholeRowIterator.decodeRow(k, v);
      final TabletMetadata tm = TabletMetadata.convertRow(decodedRow.entrySet().iterator(),
          TabletManagement.CONFIGURED_COLUMNS, false, true);

      actions.clear();
      if (managerState != ManagerState.NORMAL || current.isEmpty() || onlineTables.isEmpty()) {
        // when manager is in the process of starting up or shutting down return everything.
        actions.add(ManagementAction.NEEDS_LOCATION_UPDATE);
      } else {
        LOG.trace("Evaluating extent: {}", tm);
        computeTabletManagementActions(tm, actions);
      }

      if (!actions.isEmpty()) {
        // If we simply returned here, then the client would get the encoded K,V
        // from the WholeRowIterator. However, it would not know the reason(s) why
        // it was returned. Insert a K,V pair to represent the reasons. The client
        // can pull this K,V pair from the results by looking at the colf.
        TabletManagement.addActions(decodedRow, actions);
        topKey = decodedRow.firstKey();
        topValue = WholeRowIterator.encodeRow(new ArrayList<>(decodedRow.keySet()),
            new ArrayList<>(decodedRow.values()));
        LOG.trace("Returning extent {} with reasons: {}", tm.getExtent(), actions);
        return;
      }

      LOG.trace("No reason to return extent {}, continuing", tm.getExtent());
      getSource().next();
    }
  }

  /**
   * Evaluates whether or not this Tablet should be returned so that it can be acted upon by the
   * Manager
   */
  private void computeTabletManagementActions(final TabletMetadata tm,
      final Set<ManagementAction> reasonsToReturnThisTablet) {

    if (tm.isFutureAndCurrentLocationSet()) {
      // no need to check everything, we are in a known state where we want to return everything.
      reasonsToReturnThisTablet.add(ManagementAction.BAD_STATE);
      return;
    }

    // we always want data about merges
    final MergeInfo merge = merges.get(tm.getTableId());
    if (merge != null) {
      // could make this smarter by only returning if the tablet is involved in the merge
      reasonsToReturnThisTablet.add(ManagementAction.IS_MERGING);
    }

    if (shouldReturnDueToLocation(tm, onlineTables, current)) {
      reasonsToReturnThisTablet.add(ManagementAction.NEEDS_LOCATION_UPDATE);
    }

    if (tm.getOperationId() == null) {
      try {
        final long splitThreshold =
            ConfigurationTypeHelper.getFixedMemoryAsBytes(this.env.getPluginEnv()
                .getConfiguration(tm.getTableId()).get(Property.TABLE_SPLIT_THRESHOLD.getKey()));
        if (shouldReturnDueToSplit(tm, splitThreshold)) {
          reasonsToReturnThisTablet.add(ManagementAction.NEEDS_SPLITTING);
        }
        // important to call this since reasonsToReturnThisTablet is passed to it
        if (!compactionGenerator
            .generateJobs(tm, determineCompactionKinds(reasonsToReturnThisTablet)).isEmpty()) {
          reasonsToReturnThisTablet.add(ManagementAction.NEEDS_COMPACTING);
        }
      } catch (NullPointerException e) {
        LOG.info(
            "Unable to determine if tablet {} should split or compact, maybe table was deleted?",
            tm.getExtent());
      }
    }
  }

  private static final Set<CompactionKind> ALL_COMPACTION_KINDS =
      Collections.unmodifiableSet(EnumSet.allOf(CompactionKind.class));
  private static final Set<CompactionKind> SPLIT_COMPACTION_KINDS;

  static {
    var tmp = EnumSet.allOf(CompactionKind.class);
    tmp.remove(CompactionKind.SYSTEM);
    SPLIT_COMPACTION_KINDS = Collections.unmodifiableSet(tmp);
  }

  public static Set<CompactionKind>
      determineCompactionKinds(Set<ManagementAction> reasonsToReturnThisTablet) {
    if (reasonsToReturnThisTablet.contains(ManagementAction.NEEDS_SPLITTING)) {
      return SPLIT_COMPACTION_KINDS;
    } else {
      return ALL_COMPACTION_KINDS;
    }
  }
}
