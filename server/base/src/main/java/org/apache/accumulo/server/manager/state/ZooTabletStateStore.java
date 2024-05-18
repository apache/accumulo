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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

class ZooTabletStateStore extends AbstractTabletStateStore implements TabletStateStore {

  private static final Logger log = LoggerFactory.getLogger(ZooTabletStateStore.class);
  private final DataLevel level;
  private final ServerContext ctx;

  ZooTabletStateStore(DataLevel level, ServerContext context) {
    super(context);
    this.ctx = context;
    this.level = level;
  }

  @Override
  public DataLevel getLevel() {
    return level;
  }

  @Override
  public ClosableIterator<TabletManagement> iterator(List<Range> ranges,
      TabletManagementParameters parameters) {
    Preconditions.checkArgument(parameters.getLevel() == getLevel());

    final String zpath = ctx.getZooKeeperRoot() + RootTable.ZROOT_TABLET;
    final TabletIteratorEnvironment env = new TabletIteratorEnvironment(ctx, IteratorScope.scan,
        ctx.getTableConfiguration(AccumuloTable.ROOT.tableId()), AccumuloTable.ROOT.tableId());
    final TabletManagementIterator tmi = new TabletManagementIterator();
    final AtomicBoolean closed = new AtomicBoolean(false);

    try {
      final byte[] rootTabletMetadata =
          ctx.getZooReaderWriter().getZooKeeper().getData(zpath, false, null);
      final RootTabletMetadata rtm = new RootTabletMetadata(new String(rootTabletMetadata, UTF_8));
      final SortedMapIterator iter = new SortedMapIterator(rtm.toKeyValues());
      tmi.init(iter,
          Map.of(TabletManagementIterator.TABLET_GOAL_STATE_PARAMS_OPTION, parameters.serialize()),
          env);
      tmi.seek(new Range(), null, true);
    } catch (KeeperException | InterruptedException | IOException e2) {
      throw new IllegalStateException(
          "Error setting up TabletManagementIterator for the root tablet", e2);
    }

    return new ClosableIterator<TabletManagement>() {

      @Override
      public void close() {
        closed.compareAndSet(false, true);
      }

      @Override
      public boolean hasNext() {
        if (closed.get()) {
          return false;
        }

        boolean result = tmi.hasTop();
        if (!result) {
          close();
        }
        return result;
      }

      @Override
      public TabletManagement next() {
        if (closed.get() || !tmi.hasTop()) {
          throw new NoSuchElementException(this.getClass().getSimpleName() + " is closed");
        }

        Key k = tmi.getTopKey();
        Value v = tmi.getTopValue();
        Entry<Key,Value> e = new AbstractMap.SimpleImmutableEntry<>(k, v);
        try {
          tmi.next();
          Preconditions.checkState(!tmi.hasTop(),
              "Saw multiple tablet metadata entries for root table");
          TabletManagement tm = TabletManagementIterator.decode(e);
          log.trace(
              "Returning metadata tablet, extent: {}, hostingGoal: {}, actions: {}, error: {}",
              tm.getTabletMetadata().getExtent(), tm.getTabletMetadata().getTabletAvailability(),
              tm.getActions(), tm.getErrorMessage());
          return tm;
        } catch (IOException e1) {
          throw new UncheckedIOException("Error creating TabletMetadata object for root tablet",
              e1);
        }
      }
    };

  }

  private static void validateAssignments(Collection<Assignment> assignments) {
    if (assignments.size() != 1) {
      throw new IllegalArgumentException("There is only one root tablet");
    }
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(RootTable.EXTENT) != 0) {
      throw new IllegalArgumentException("You can only store the root tablet location");
    }
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments)
      throws DistributedStoreException {
    validateAssignments(assignments);
    super.setFutureLocations(assignments);
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    validateAssignments(assignments);
    super.setLocations(assignments);
  }

  private static void validateTablets(Collection<TabletMetadata> tablets) {
    if (tablets.size() != 1) {
      throw new IllegalArgumentException("There is only one root tablet");
    }
    TabletMetadata tm = tablets.iterator().next();
    if (tm.getExtent().compareTo(RootTable.EXTENT) != 0) {
      throw new IllegalArgumentException("You can only store the root tablet location");
    }
  }

  @Override
  public void unassign(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    validateTablets(tablets);

    super.unassign(tablets, logsForDeadServers);

    log.debug("unassign root tablet location");
  }

  @Override
  public void suspend(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, SteadyTime suspensionTimestamp)
      throws DistributedStoreException {
    validateTablets(tablets);
    super.suspend(tablets, logsForDeadServers, suspensionTimestamp);
  }

  @Override
  protected void processSuspension(Ample.ConditionalTabletMutator tabletMutator, TabletMetadata tm,
      SteadyTime suspensionTimestamp) {
    // No support for suspending root tablet, so this is a NOOP
  }

  @Override
  public void unsuspend(Collection<TabletMetadata> tablets) {
    // no support for suspending root tablet.
  }

  @Override
  public String name() {
    return "Root Table";
  }
}
