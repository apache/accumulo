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
package org.apache.accumulo.manager.compaction.coordinator.commit;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;

import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.AbstractTabletFile;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CommitCompaction extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CommitCompaction.class);
  private final CompactionCommitData commitData;
  private final String newDatafile;

  public CommitCompaction(CompactionCommitData commitData, String newDatafile) {
    this.commitData = commitData;
    this.newDatafile = newDatafile;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    var ecid = ExternalCompactionId.of(commitData.ecid);
    var newFile = Optional.ofNullable(newDatafile).map(f -> ReferencedTabletFile.of(new Path(f)));

    // ELASTICITIY_TODO is it possible to test this code running a 2nd time, simulating a failure
    // and rerun? Maybe fate could have a testing mode where it calls operations multiple times?

    // It is possible that when this runs that the compaction was previously committed and then the
    // process died and now its running again. In this case commit should do nothing, but its
    // important to still carry on with the rest of the steps after commit. This code ignores a that
    // fact that a commit may not have happened in the current call and continues for this reason.
    TabletMetadata tabletMetadata = commitCompaction(manager.getContext(), ecid, newFile);

    String loc = null;
    if (tabletMetadata != null && tabletMetadata.getLocation() != null) {
      loc = tabletMetadata.getLocation().getHostPortSession();
    }

    // This will causes the tablet to be reexamined to see if it needs any more compactions.
    var extent = KeyExtent.fromThrift(commitData.textent);
    manager.getEventCoordinator().event(extent, "Compaction completed %s", extent);

    return new PutGcCandidates(commitData, loc);
  }

  KeyExtent getExtent() {
    return KeyExtent.fromThrift(commitData.textent);
  }

  private TabletMetadata commitCompaction(ServerContext ctx, ExternalCompactionId ecid,
      Optional<ReferencedTabletFile> newDatafile) {

    var tablet =
        ctx.getAmple().readTablet(getExtent(), ECOMP, SELECTED, LOCATION, FILES, COMPACTED, OPID);

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(100))
        .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofSeconds(10)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    while (canCommitCompaction(ecid, tablet)) {
      CompactionMetadata ecm = tablet.getExternalCompactions().get(ecid);

      // the compacted files should not exists in the tablet already
      var tablet2 = tablet;
      newDatafile.ifPresent(
          newFile -> Preconditions.checkState(!tablet2.getFiles().contains(newFile.insert()),
              "File already exists in tablet %s %s", newFile, tablet2.getFiles()));

      try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
        var tabletMutator = tabletsMutator.mutateTablet(getExtent()).requireAbsentOperation()
            .requireCompaction(ecid).requireSame(tablet, FILES, LOCATION);

        if (ecm.getKind() == CompactionKind.USER) {
          tabletMutator.requireSame(tablet, SELECTED, COMPACTED);
        }

        // make the needed updates to the tablet
        updateTabletForCompaction(commitData.stats, ecid, tablet, newDatafile, ecm, tabletMutator);

        tabletMutator
            .submit(tabletMetadata -> !tabletMetadata.getExternalCompactions().containsKey(ecid));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Compaction completed {} added {} removed {}", tablet.getExtent(), newDatafile,
              ecm.getJobFiles().stream().map(AbstractTabletFile::getFileName)
                  .collect(Collectors.toList()));
        }

        var result = tabletsMutator.process().get(getExtent());
        if (result.getStatus() == Ample.ConditionalResult.Status.ACCEPTED) {
          // Compaction was successfully committed to the tablet so log it
          TabletLogger.compacted(getExtent(), ecid, commitData.kind, commitData.getJobFiles(),
              newDatafile);
          break;
        } else {
          // compaction failed to commit, maybe something changed on the tablet so lets reread the
          // metadata and try again
          tablet = result.readMetadata();
        }

        retry.waitForNextAttempt(LOG, "Failed to commit " + ecid + " for tablet " + getExtent());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    return tablet;
  }

  private void updateTabletForCompaction(TCompactionStats stats, ExternalCompactionId ecid,
      TabletMetadata tablet, Optional<ReferencedTabletFile> newDatafile, CompactionMetadata ecm,
      Ample.ConditionalTabletMutator tabletMutator) {

    if (ecm.getKind() == CompactionKind.USER) {
      if (tablet.getSelectedFiles().getFiles().equals(ecm.getJobFiles())) {
        // all files selected for the user compactions are finished, so the tablet is finish and
        // its compaction id needs to be updated.

        FateId fateId = tablet.getSelectedFiles().getFateId();

        Preconditions.checkArgument(!tablet.getCompacted().contains(fateId),
            "Tablet %s unexpected has selected files and compacted columns for %s",
            tablet.getExtent(), fateId);

        LOG.trace("All selected files compacted for {} setting compacted for {}",
            tablet.getExtent(), tablet.getSelectedFiles().getFateId());

        tabletMutator.deleteSelectedFiles();
        tabletMutator.putCompacted(fateId);

      } else {
        // not all of the selected files were finished, so need to add the new file to the
        // selected set

        Set<StoredTabletFile> newSelectedFileSet =
            new HashSet<>(tablet.getSelectedFiles().getFiles());
        newSelectedFileSet.removeAll(ecm.getJobFiles());

        if (newDatafile.isPresent()) {
          LOG.trace(
              "Not all selected files for {} are done, adding new selected file {} from compaction",
              tablet.getExtent(), newDatafile.orElseThrow().getPath().getName());
          newSelectedFileSet.add(newDatafile.orElseThrow().insert());
        } else {
          LOG.trace(
              "Not all selected files for {} are done, compaction produced no output so not adding to selected set.",
              tablet.getExtent());
        }

        tabletMutator.putSelectedFiles(new SelectedFiles(newSelectedFileSet,
            tablet.getSelectedFiles().initiallySelectedAll(), tablet.getSelectedFiles().getFateId(),
            tablet.getSelectedFiles().getCompletedJobs() + 1,
            tablet.getSelectedFiles().getSelectedTime()));
      }
    }

    if (tablet.getLocation() != null) {
      // add scan entries to prevent GC in case the hosted tablet is currently using the files for
      // scan
      ecm.getJobFiles().forEach(tabletMutator::putScan);
    }
    ecm.getJobFiles().forEach(tabletMutator::deleteFile);
    tabletMutator.deleteExternalCompaction(ecid);

    if (newDatafile.isPresent()) {
      tabletMutator.putFile(newDatafile.orElseThrow(),
          new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()));
    }
  }

  // ELASTICITY_TODO unit test this method
  public static boolean canCommitCompaction(ExternalCompactionId ecid,
      TabletMetadata tabletMetadata) {

    if (tabletMetadata == null) {
      LOG.debug("Received completion notification for nonexistent tablet {}", ecid);
      return false;
    }

    var extent = tabletMetadata.getExtent();

    if (tabletMetadata.getOperationId() != null) {
      // split, merge, and delete tablet should delete the compaction entry in the tablet
      LOG.debug("Received completion notification for tablet with active operation {} {} {}", ecid,
          extent, tabletMetadata.getOperationId());
      return false;
    }

    CompactionMetadata ecm = tabletMetadata.getExternalCompactions().get(ecid);

    if (ecm == null) {
      LOG.debug("Received completion notification for unknown compaction {} {}", ecid, extent);
      return false;
    }

    if (ecm.getKind() == CompactionKind.USER) {
      if (tabletMetadata.getSelectedFiles() == null) {
        // when the compaction is canceled, selected files are deleted
        LOG.debug(
            "Received completion notification for user compaction and tablet has no selected files {} {}",
            ecid, extent);
        return false;
      }

      if (!ecm.getFateId().equals(tabletMetadata.getSelectedFiles().getFateId())) {
        // maybe the compaction was cancled and another user compaction was started on the tablet.
        LOG.debug(
            "Received completion notification for user compaction where its fate txid did not match the tablets {} {} {} {}",
            ecid, extent, ecm.getFateId(), tabletMetadata.getSelectedFiles().getFateId());
      }

      if (!tabletMetadata.getSelectedFiles().getFiles().containsAll(ecm.getJobFiles())) {
        // this is not expected to happen
        LOG.error("User compaction contained files not in the selected set {} {} {} {} {}",
            tabletMetadata.getExtent(), ecid, ecm.getKind(),
            Optional.ofNullable(tabletMetadata.getSelectedFiles()).map(SelectedFiles::getFiles),
            ecm.getJobFiles());
        return false;
      }
    }

    if (!tabletMetadata.getFiles().containsAll(ecm.getJobFiles())) {
      // this is not expected to happen
      LOG.error("Compaction contained files not in the tablet files set {} {} {} {}",
          tabletMetadata.getExtent(), ecid, tabletMetadata.getFiles(), ecm.getJobFiles());
      return false;
    }

    return true;
  }
}
