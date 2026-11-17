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
package org.apache.accumulo.gc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.metadata.schema.Ample.GcCandidateType;
import org.apache.accumulo.core.trace.TraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class GarbageCollectionAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(GarbageCollectionAlgorithm.class);

  private void removeCandidatesInUse(GarbageCollectionEnvironment gce, GcCandidates candidates)
      throws InterruptedException {

    List<GcCandidate> candidateEntriesToBeDeleted = new ArrayList<>();
    Set<TableId> tableIdsBefore = gce.getCandidateTableIDs();
    Set<TableId> tableIdsSeen = new HashSet<>(tableIdsBefore.size());
    try (Stream<Reference> references = gce.getReferences()) {
      references.forEach(ref -> {
        if (ref.isDirectory()) {
          tableIdsSeen.add(ref.getTableId());
        }

        candidates.remove(ref, candidate -> {
          log.debug("{} candidate is still in use : {}", candidate.getType(), candidate.getPath());
          if (candidate.getType() == GcCandidate.Type.TABLET_FILE && !ref.isScan()) {
            // Prevent deletion of candidates that are still in use by scans, because they won't be
            // recreated once the scan is finished.
            candidateEntriesToBeDeleted.add(candidate);
          }
        });
      });
    }
    Set<TableId> tableIdsAfter = gce.getCandidateTableIDs();
    ensureAllTablesChecked(Collections.unmodifiableSet(tableIdsBefore),
        Collections.unmodifiableSet(tableIdsSeen), Collections.unmodifiableSet(tableIdsAfter));
    gce.deleteGcCandidates(candidateEntriesToBeDeleted, GcCandidateType.INUSE);
  }

  private long removeBlipCandidates(GarbageCollectionEnvironment gce, GcCandidates candidates)
      throws TableNotFoundException {
    long blipCount = 0;

    try (Stream<String> blipStream = gce.getBlipPaths()) {
      Iterator<String> blipiter = blipStream.iterator();

      // WARNING: This block is IMPORTANT
      // You MUST REMOVE candidates that are in the same folder as a bulk
      // processing flag!

      while (blipiter.hasNext()) {
        blipCount++;
        var blipPath = blipiter.next();
        int count = candidates.removeBlip(blipPath);

        if (count > 0) {
          log.debug("Folder has bulk processing flag: {}", blipPath);
        }
      }
    }

    return blipCount;
  }

  /**
   * Double check no tables were missed during GC
   */
  @VisibleForTesting
  protected void ensureAllTablesChecked(Set<TableId> tableIdsBefore, Set<TableId> tableIdsSeen,
      Set<TableId> tableIdsAfter) {

    // if a table was added or deleted during this run, it is acceptable to not
    // have seen those tables ids when scanning the metadata table. So get the intersection
    final Set<TableId> tableIdsMustHaveSeen = new HashSet<>(tableIdsBefore);
    tableIdsMustHaveSeen.retainAll(tableIdsAfter);

    if (tableIdsMustHaveSeen.isEmpty() && !tableIdsSeen.isEmpty()) {
      // This exception will end up terminating the current GC loop iteration
      // in SimpleGarbageCollector.run and will be logged. SimpleGarbageCollector
      // will start the loop again.
      throw new RuntimeException("Garbage collection will not proceed because "
          + "table ids were seen in the metadata table and none were seen Zookeeper. "
          + "This can have two causes. First, total number of tables going to/from "
          + "zero during a GC cycle will cause this. Second, it could be caused by "
          + "corruption of the metadata table and/or Zookeeper. Only the second cause "
          + "is problematic, but there is no way to distinguish between the two causes "
          + "so this GC cycle will not proceed. The first cause should be transient "
          + "and one would not expect to see this message repeated in subsequent GC cycles.");
    }

    // From that intersection, remove all the table ids that were seen.
    tableIdsMustHaveSeen.removeAll(tableIdsSeen);

    // If anything is left then we missed a table and may not have removed rfiles references
    // from the candidates list that are actually still in use, which would
    // result in the rfiles being deleted in the next step of the GC process
    if (!tableIdsMustHaveSeen.isEmpty()) {
      // maybe a scan failed?
      throw new IllegalStateException("Saw table IDs in ZK that were not in metadata table:  "
          + tableIdsMustHaveSeen + " TableIDs before GC: " + tableIdsBefore
          + ", TableIDs during GC: " + tableIdsSeen + ", TableIDs after GC: " + tableIdsAfter);
    }
  }

  private long confirmDeletesTrace(GarbageCollectionEnvironment gce, GcCandidates candidateMap)
      throws InterruptedException, TableNotFoundException {
    long blips = 0;
    Span confirmDeletesSpan = TraceUtil.startSpan(this.getClass(), "confirmDeletes");
    try (Scope scope = confirmDeletesSpan.makeCurrent()) {
      blips = removeBlipCandidates(gce, candidateMap);
      removeCandidatesInUse(gce, candidateMap);
    } catch (Exception e) {
      TraceUtil.setException(confirmDeletesSpan, e, true);
      throw e;
    } finally {
      confirmDeletesSpan.end();
    }
    return blips;
  }

  private void deleteConfirmedCandidates(GarbageCollectionEnvironment gce, GcCandidates candidates)
      throws InterruptedException, IOException, TableNotFoundException {
    Span deleteSpan = TraceUtil.startSpan(this.getClass(), "deleteFiles");
    try (Scope deleteScope = deleteSpan.makeCurrent()) {
      gce.deleteConfirmedCandidates(candidates);
    } catch (Exception e) {
      TraceUtil.setException(deleteSpan, e, true);
      throw e;
    } finally {
      deleteSpan.end();
    }
  }

  public long collect(GarbageCollectionEnvironment gce)
      throws InterruptedException, TableNotFoundException, IOException {

    Iterator<GcCandidate> candidatesIter = gce.getCandidates();
    long totalBlips = 0;
    int batchCount = 0;

    while (candidatesIter.hasNext()) {
      List<GcCandidate> batchOfCandidates;
      Span candidatesSpan = TraceUtil.startSpan(this.getClass(), "getCandidates");
      try (Scope candidatesScope = candidatesSpan.makeCurrent()) {
        batchOfCandidates = gce.readCandidatesThatFitInMemory(candidatesIter);
      } catch (Exception e) {
        TraceUtil.setException(candidatesSpan, e, true);
        throw e;
      } finally {
        candidatesSpan.end();
      }
      batchCount++;
      totalBlips = deleteBatch(gce, batchOfCandidates, batchCount);
    }
    return totalBlips;
  }

  /**
   * Given a sub-list of possible deletion candidates, process and remove valid deletion candidates.
   */
  private long deleteBatch(GarbageCollectionEnvironment gce, List<GcCandidate> currentBatch,
      int batchCount) throws InterruptedException, TableNotFoundException, IOException {

    long origSize = currentBatch.size();
    gce.incrementCandidatesStat(origSize);
    log.info("Batch {} total deletion candidates: {}", batchCount, origSize);

    GcCandidates candidates = new GcCandidates(currentBatch);

    long blips = confirmDeletesTrace(gce, candidates);
    gce.incrementInUseStat(origSize - candidates.size());

    deleteConfirmedCandidates(gce, candidates);

    return blips;
  }
}
