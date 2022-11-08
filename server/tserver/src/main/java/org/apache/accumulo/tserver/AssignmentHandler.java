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
package org.apache.accumulo.tserver;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.server.problems.ProblemType.TABLET_LOAD;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.manager.state.Assignment;
import org.apache.accumulo.server.manager.state.TabletStateStore;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.util.ManagerMetadataUtil;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.managermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletData;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AssignmentHandler implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(AssignmentHandler.class);
  private static final String METADATA_ISSUE = "Saw metadata issue when loading tablet : ";
  private final KeyExtent extent;
  private final int retryAttempt;
  private final TabletServer server;

  public AssignmentHandler(TabletServer server, KeyExtent extent) {
    this(server, extent, 0);
  }

  public AssignmentHandler(TabletServer server, KeyExtent extent, int retryAttempt) {
    this.server = server;
    this.extent = extent;
    this.retryAttempt = retryAttempt;
  }

  @Override
  public void run() {
    synchronized (server.unopenedTablets) {
      synchronized (server.openingTablets) {
        synchronized (server.onlineTablets) {
          // nothing should be moving between sets, do a sanity
          // check
          Set<KeyExtent> unopenedOverlapping =
              KeyExtent.findOverlapping(extent, server.unopenedTablets);
          Set<KeyExtent> openingOverlapping =
              KeyExtent.findOverlapping(extent, server.openingTablets);
          Set<KeyExtent> onlineOverlapping =
              KeyExtent.findOverlapping(extent, server.onlineTablets.snapshot());

          if (openingOverlapping.contains(extent) || onlineOverlapping.contains(extent)) {
            return;
          }

          if (!unopenedOverlapping.contains(extent)) {
            log.info("assignment {} no longer in the unopened set", extent);
            return;
          }

          if (unopenedOverlapping.size() != 1 || !openingOverlapping.isEmpty()
              || !onlineOverlapping.isEmpty()) {
            throw new IllegalStateException(
                "overlaps assigned " + extent + " " + !server.unopenedTablets.contains(extent) + " "
                    + unopenedOverlapping + " " + openingOverlapping + " " + onlineOverlapping);
          }
        }

        server.unopenedTablets.remove(extent);
        server.openingTablets.add(extent);
      }
    }

    // check Metadata table before accepting assignment
    Text locationToOpen = null;
    TabletMetadata tabletMetadata = null;
    boolean canLoad = false;
    try {
      tabletMetadata = server.getContext().getAmple().readTablet(extent);

      canLoad = checkTabletMetadata(extent, server.getTabletSession(), tabletMetadata);

      if (canLoad && tabletMetadata.sawOldPrevEndRow()) {
        KeyExtent fixedExtent =
            ManagerMetadataUtil.fixSplit(server.getContext(), tabletMetadata, server.getLock());

        synchronized (server.openingTablets) {
          server.openingTablets.remove(extent);
          server.openingTablets.notifyAll();
          // it expected that the new extent will overlap the old one... if it does not, it
          // should not be added to unopenedTablets
          if (!KeyExtent.findOverlapping(extent, new TreeSet<>(Arrays.asList(fixedExtent)))
              .contains(fixedExtent)) {
            throw new IllegalStateException(
                "Fixed split does not overlap " + extent + " " + fixedExtent);
          }
          server.unopenedTablets.add(fixedExtent);
        }
        // split was rolled back... try again
        new AssignmentHandler(server, fixedExtent).run();
        return;

      }
    } catch (Exception e) {
      synchronized (server.openingTablets) {
        server.openingTablets.remove(extent);
        server.openingTablets.notifyAll();
      }
      log.warn("Failed to verify tablet " + extent, e);
      server.enqueueManagerMessage(new TabletStatusMessage(TabletLoadState.LOAD_FAILURE, extent));
      throw new RuntimeException(e);
    }

    if (!canLoad) {
      log.debug("Reporting tablet {} assignment failure: unable to verify Tablet Information",
          extent);
      synchronized (server.openingTablets) {
        server.openingTablets.remove(extent);
        server.openingTablets.notifyAll();
      }
      server.enqueueManagerMessage(new TabletStatusMessage(TabletLoadState.LOAD_FAILURE, extent));
      return;
    }

    Tablet tablet = null;
    boolean successful = false;

    try {
      server.acquireRecoveryMemory(extent);

      TabletResourceManager trm = server.resourceManager.createTabletResourceManager(extent,
          server.getTableConfiguration(extent));
      TabletData data = new TabletData(tabletMetadata);

      tablet = new Tablet(server, extent, trm, data);
      // If a minor compaction starts after a tablet opens, this indicates a log recovery
      // occurred. This recovered data must be minor compacted.
      // There are three reasons to wait for this minor compaction to finish before placing the
      // tablet in online tablets.
      //
      // 1) The log recovery code does not handle data written to the tablet on multiple tablet
      // servers.
      // 2) The log recovery code does not block if memory is full. Therefore recovering lots of
      // tablets that use a lot of memory could run out of memory.
      // 3) The minor compaction finish event did not make it to the logs (the file will be in
      // metadata, preventing replay of compacted data)... but do not
      // want a majc to wipe the file out from metadata and then have another process failure...
      // this could cause duplicate data to replay.
      if (tablet.getNumEntriesInMemory() > 0
          && !tablet.minorCompactNow(MinorCompactionReason.RECOVERY)) {
        throw new RuntimeException("Minor compaction after recovery fails for " + extent);
      }
      Assignment assignment = new Assignment(extent, server.getTabletSession());
      TabletStateStore.setLocation(server.getContext(), assignment);

      synchronized (server.openingTablets) {
        synchronized (server.onlineTablets) {
          server.openingTablets.remove(extent);
          server.onlineTablets.put(extent, tablet);
          server.openingTablets.notifyAll();
          server.recentlyUnloadedCache.remove(tablet.getExtent());
        }
      }
      tablet = null; // release this reference
      successful = true;
    } catch (Exception e) {
      log.warn("exception trying to assign tablet {} {}", extent, locationToOpen, e);

      if (e.getMessage() != null) {
        log.warn("{}", e.getMessage());
      }

      TableId tableId = extent.tableId();
      ProblemReports.getInstance(server.getContext()).report(new ProblemReport(tableId, TABLET_LOAD,
          extent.getUUID().toString(), server.getClientAddressString(), e));
    } finally {
      server.releaseRecoveryMemory(extent);
    }

    if (successful) {
      server.enqueueManagerMessage(new TabletStatusMessage(TabletLoadState.LOADED, extent));
    } else {
      synchronized (server.unopenedTablets) {
        synchronized (server.openingTablets) {
          server.openingTablets.remove(extent);
          server.unopenedTablets.add(extent);
          server.openingTablets.notifyAll();
        }
      }
      log.warn("failed to open tablet {} reporting failure to manager", extent);
      server.enqueueManagerMessage(new TabletStatusMessage(TabletLoadState.LOAD_FAILURE, extent));
      long reschedule = Math.min((1L << Math.min(32, retryAttempt)) * 1000, MINUTES.toMillis(10));
      log.warn(String.format("rescheduling tablet load in %.2f seconds", reschedule / 1000.));
      ThreadPools.watchCriticalScheduledTask(
          this.server.getContext().getScheduledExecutor().schedule(new Runnable() {
            @Override
            public void run() {
              log.info("adding tablet {} back to the assignment pool (retry {})", extent,
                  retryAttempt);
              AssignmentHandler handler = new AssignmentHandler(server, extent, retryAttempt + 1);
              if (extent.isMeta()) {
                if (extent.isRootTablet()) {
                  Threads.createThread("Root tablet assignment retry", handler).start();
                } else {
                  server.resourceManager.addMetaDataAssignment(extent, log, handler);
                }
              } else {
                server.resourceManager.addAssignment(extent, log, handler);
              }
            }
          }, reschedule, TimeUnit.MILLISECONDS));
    }
  }

  public static boolean checkTabletMetadata(KeyExtent extent, TServerInstance instance,
      TabletMetadata meta) throws AccumuloException {
    return checkTabletMetadata(extent, instance, meta, false);
  }

  public static boolean checkTabletMetadata(KeyExtent extent, TServerInstance instance,
      TabletMetadata meta, boolean ignoreLocationCheck) throws AccumuloException {

    if (meta == null) {
      log.info(METADATA_ISSUE + "{}, its metadata was not found.", extent);
      return false;
    }

    if (!meta.sawPrevEndRow()) {
      throw new AccumuloException(METADATA_ISSUE + "metadata entry does not have prev row ("
          + meta.getTableId() + " " + meta.getEndRow() + ")");
    }

    if (!extent.equals(meta.getExtent())) {
      log.info(METADATA_ISSUE + "tablet extent mismatch {} {}", extent, meta.getExtent());
      return false;
    }

    if (meta.getDirName() == null) {
      throw new AccumuloException(
          METADATA_ISSUE + "metadata entry does not have directory (" + meta.getExtent() + ")");
    }

    if (meta.getTime() == null && !extent.equals(RootTable.EXTENT)) {
      throw new AccumuloException(
          METADATA_ISSUE + "metadata entry does not have time (" + meta.getExtent() + ")");
    }

    TabletMetadata.Location loc = meta.getLocation();

    if (!ignoreLocationCheck && (loc == null || loc.getType() != TabletMetadata.LocationType.FUTURE
        || !instance.equals(loc))) {
      log.info(METADATA_ISSUE + "Unexpected location {} {}", extent, loc);
      return false;
    }

    return true;
  }
}
