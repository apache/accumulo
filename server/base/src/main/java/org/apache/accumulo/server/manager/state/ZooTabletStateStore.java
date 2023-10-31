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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.ReadConsistency;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.compaction.CompactionJobGenerator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

class ZooTabletStateStore extends AbstractTabletStateStore implements TabletStateStore {

  private static final Logger log = LoggerFactory.getLogger(ZooTabletStateStore.class);
  private final Ample ample;
  private final DataLevel level;
  private final ServerContext ctx;

  ZooTabletStateStore(DataLevel level, ServerContext context) {
    super(context);
    this.ctx = context;
    this.level = level;
    this.ample = context.getAmple();
  }

  @Override
  public DataLevel getLevel() {
    return level;
  }

  @Override
  public ClosableIterator<TabletManagement> iterator(List<Range> ranges,
      TabletManagementParameters parameters) {
    Preconditions.checkArgument(parameters.getLevel() == getLevel());

    return new ClosableIterator<>() {
      boolean finished = false;

      @Override
      public boolean hasNext() {
        return !finished;
      }

      @Override
      public TabletManagement next() {
        finished = true;

        final var actions = EnumSet.of(ManagementAction.NEEDS_LOCATION_UPDATE);
        final TabletMetadata tm = ample.readTablet(RootTable.EXTENT, ReadConsistency.EVENTUAL);
        String error = null;
        try {
          CompactionJobGenerator cjg =
              new CompactionJobGenerator(new ServiceEnvironmentImpl(ctx), Map.of());
          var jobs = cjg.generateJobs(tm,
              EnumSet.of(CompactionKind.SYSTEM, CompactionKind.USER, CompactionKind.SELECTOR));
          if (!jobs.isEmpty()) {
            actions.add(ManagementAction.NEEDS_COMPACTING);
          }
        } catch (Exception e) {
          log.error("Error computing tablet management actions for Root extent", e);
          error = e.getMessage();
        }
        return new TabletManagement(actions, tm, error);

      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {}
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
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    validateTablets(tablets);
    super.suspend(tablets, logsForDeadServers, suspensionTimestamp);
  }

  @Override
  protected void processSuspension(Ample.ConditionalTabletMutator tabletMutator, TabletMetadata tm,
      long suspensionTimestamp) {
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
