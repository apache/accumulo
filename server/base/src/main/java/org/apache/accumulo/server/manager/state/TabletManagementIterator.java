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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.PluginEnvironment.Configuration;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.core.spi.balancer.SimpleLoadBalancer;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.server.compaction.CompactionJobGenerator;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;
import org.apache.accumulo.server.manager.balancer.BalancerEnvironmentImpl;
import org.apache.accumulo.server.split.SplitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator used by the TabletGroupWatcher threads in the Manager. This iterator returns
 * TabletManagement objects for each Tablet that needs some type of action performed on it by the
 * Manager.
 */
public class TabletManagementIterator extends SkippingIterator {
  private static final Logger LOG = LoggerFactory.getLogger(TabletManagementIterator.class);
  public static final String TABLET_GOAL_STATE_PARAMS_OPTION = "tgsParams";
  private CompactionJobGenerator compactionGenerator;
  private TabletBalancer balancer;

  private static boolean shouldReturnDueToSplit(final TabletMetadata tm,
      final Configuration tableConfig) {

    final long splitThreshold = ConfigurationTypeHelper
        .getFixedMemoryAsBytes(tableConfig.get(Property.TABLE_SPLIT_THRESHOLD.getKey()));
    final long maxEndRowSize = ConfigurationTypeHelper
        .getFixedMemoryAsBytes(tableConfig.get(Property.TABLE_MAX_END_ROW_SIZE.getKey()));
    final int maxFilesToOpen = (int) ConfigurationTypeHelper.getFixedMemoryAsBytes(
        tableConfig.get(Property.TSERV_TABLET_SPLIT_FINDMIDPOINT_MAXOPEN.getKey()));

    // If the current computed metadata matches the current marker then we can't split,
    // so we return false. If the marker is set but doesn't match then return true
    // which gives a chance to clean up the marker and recheck.
    var unsplittable = tm.getUnSplittable();
    if (unsplittable != null) {
      return !unsplittable.equals(UnSplittableMetadata.toUnSplittable(tm.getExtent(),
          splitThreshold, maxEndRowSize, maxFilesToOpen, tm.getFiles()));
    }

    // If unsplittable is not set at all then check if over split threshold
    final boolean shouldSplit = SplitUtils.needsSplit(tableConfig, tm);
    LOG.trace("{} should split? sum: {}, threshold: {}, result: {}", tm.getExtent(),
        tm.getFileSize(), splitThreshold, shouldSplit);
    return shouldSplit;
  }

  private boolean shouldReturnDueToLocation(final TabletMetadata tm) {

    if (tm.getExtent().isRootTablet()) {
      return true;
    }

    if (tabletMgmtParams.getMigrations().containsKey(tm.getExtent())) {
      // Ideally only the state and goalState would need to be used to determine if a tablet should
      // be returned. However, the Manager/TGW currently needs everything in the migrating set
      // returned so it can update in memory maps it has. If this were improved then this case would
      // not be needed.
      return true;
    }

    TabletState state = TabletState.compute(tm, tabletMgmtParams.getOnlineTsevers());
    TabletGoalState goalState = TabletGoalState.compute(tm, state, balancer, tabletMgmtParams);
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "extent:{} state:{} goalState:{} tabletAvailability:{}, hostingRequested: {}, opId: {}",
          tm.getExtent(), state, goalState, tm.getTabletAvailability(), tm.getHostingRequested(),
          tm.getOperationId());
    }

    switch (goalState) {
      case HOSTED:
        return state != TabletState.HOSTED;
      case SUSPENDED:
        return state != TabletState.SUSPENDED;
      case UNASSIGNED:
        return state != TabletState.UNASSIGNED;
      default:
        throw new IllegalStateException("unknown goal state " + goalState);
    }
  }

  public static void configureScanner(final ScannerBase scanner,
      final TabletManagementParameters tabletMgmtParams) {
    // Note : if the scanner is ever made to fetch columns, then TabletManagement.CONFIGURED_COLUMNS
    // must be updated
    scanner.addScanIterator(new IteratorSetting(1000, "wholeRows", WholeRowIterator.class));
    IteratorSetting tabletChange =
        new IteratorSetting(1001, "ManagerTabletInfoIterator", TabletManagementIterator.class);
    tabletChange.addOption(TABLET_GOAL_STATE_PARAMS_OPTION, tabletMgmtParams.serialize());
    scanner.addScanIterator(tabletChange);
  }

  public static TabletManagement decode(Entry<Key,Value> e) throws IOException {
    return new TabletManagement(e.getKey(), e.getValue());
  }

  private IteratorEnvironment env;
  private Key topKey = null;
  private Value topValue = null;
  private TabletManagementParameters tabletMgmtParams = null;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    this.env = env;
    tabletMgmtParams =
        TabletManagementParameters.deserialize(options.get(TABLET_GOAL_STATE_PARAMS_OPTION));
    compactionGenerator =
        new CompactionJobGenerator(env.getPluginEnv(), tabletMgmtParams.getCompactionHints());
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
      Exception error = null;
      try {
        LOG.trace("Evaluating extent: {}", tm);
        if (tm.getExtent().isMeta()) {
          computeTabletManagementActions(tm, actions);
        } else {
          if (tabletMgmtParams.getManagerState() != ManagerState.NORMAL
              || tabletMgmtParams.getOnlineTsevers().isEmpty()
              || tabletMgmtParams.getOnlineTables().isEmpty()) {
            // when manager is in the process of starting up or shutting down return everything.
            actions.add(ManagementAction.NEEDS_LOCATION_UPDATE);
          } else {
            computeTabletManagementActions(tm, actions);
          }
        }
      } catch (Exception e) {
        LOG.error("Error computing tablet management actions for extent: {}", tm.getExtent(), e);
        error = e;
      }

      if (!actions.isEmpty() || error != null) {
        if (error != null) {
          // Insert the error into K,V pair representing
          // the tablet metadata.
          TabletManagement.addError(decodedRow, error);
        } else if (!actions.isEmpty()) {
          // If we simply returned here, then the client would get the encoded K,V
          // from the WholeRowIterator. However, it would not know the reason(s) why
          // it was returned. Insert a K,V pair to represent the reasons. The client
          // can pull this K,V pair from the results by looking at the colf.
          TabletManagement.addActions(decodedRow, actions);
        }
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

  private static final Set<ManagementAction> REASONS_NOT_TO_SPLIT_OR_COMPACT =
      Collections.unmodifiableSet(EnumSet.of(ManagementAction.BAD_STATE,
          ManagementAction.NEEDS_VOLUME_REPLACEMENT, ManagementAction.NEEDS_RECOVERY));

  /**
   * Evaluates whether or not this Tablet should be returned so that it can be acted upon by the
   * Manager
   */
  private void computeTabletManagementActions(final TabletMetadata tm,
      final Set<ManagementAction> reasonsToReturnThisTablet) {

    if (tm.isFutureAndCurrentLocationSet()) {
      // no need to check everything, we are in a known state where we want to return everything.
      reasonsToReturnThisTablet.add(ManagementAction.BAD_STATE);
    }

    if (!tm.getLogs().isEmpty() && (tm.getOperationId() == null
        || tm.getOperationId().getType() != TabletOperationType.DELETING)) {
      reasonsToReturnThisTablet.add(ManagementAction.NEEDS_RECOVERY);
    }

    if (VolumeUtil.needsVolumeReplacement(tabletMgmtParams.getVolumeReplacements(), tm)) {
      reasonsToReturnThisTablet.add(ManagementAction.NEEDS_VOLUME_REPLACEMENT);
    }

    if (shouldReturnDueToLocation(tm)) {
      reasonsToReturnThisTablet.add(ManagementAction.NEEDS_LOCATION_UPDATE);
    }

    if (tm.getOperationId() == null
        && Collections.disjoint(REASONS_NOT_TO_SPLIT_OR_COMPACT, reasonsToReturnThisTablet)) {
      try {
        if (shouldReturnDueToSplit(tm, this.env.getPluginEnv().getConfiguration(tm.getTableId()))) {
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
