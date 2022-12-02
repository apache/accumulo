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

import static java.util.Arrays.stream;
import static java.util.function.Predicate.not;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.gc.ReferenceDirectory;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.trace.TraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class GarbageCollectionAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(GarbageCollectionAlgorithm.class);

  /**
   * This method takes a file or directory path and returns a relative path in 1 of 2 forms:
   *
   * <pre>
   *      1- For files: table-id/tablet-directory/filename.rf
   *      2- For directories: table-id/tablet-directory
   * </pre>
   *
   * For example, for full file path like hdfs://foo:6000/accumulo/tables/4/t0/F000.rf it will
   * return 4/t0/F000.rf. For a directory that already is relative, like 4/t0, it will just return
   * the original path. This method will also remove prefixed relative paths like ../4/t0/F000.rf
   * and return 4/t0/F000.rf. It also strips out empty tokens from paths like
   * hdfs://foo.com:6000/accumulo/tables/4//t0//F001.rf returning 4/t0/F001.rf.
   */
  private String makeRelative(String path, int expectedLen) {
    String relPath = path;

    // remove prefixed old relative path
    if (relPath.startsWith("../")) {
      relPath = relPath.substring(3);
    }

    // remove trailing slash
    while (relPath.endsWith("/")) {
      relPath = relPath.substring(0, relPath.length() - 1);
    }

    // remove beginning slash
    while (relPath.startsWith("/")) {
      relPath = relPath.substring(1);
    }

    // Handle paths like a//b///c by dropping the empty tokens.
    String[] tokens = stream(relPath.split("/")).filter(not(""::equals)).toArray(String[]::new);

    if (tokens.length > 3 && path.contains(":")) {
      // full file path like hdfs://foo:6000/accumulo/tables/4/t0/F000.rf
      if (tokens[tokens.length - 4].equals(Constants.TABLE_DIR)
          && (expectedLen == 0 || expectedLen == 3)) {
        // return the last 3 tokens after tables, like 4/t0/F000.rf
        relPath = tokens[tokens.length - 3] + "/" + tokens[tokens.length - 2] + "/"
            + tokens[tokens.length - 1];
      } else if (tokens[tokens.length - 3].equals(Constants.TABLE_DIR)
          && (expectedLen == 0 || expectedLen == 2)) {
        // return the last 2 tokens after tables, like 4/t0
        relPath = tokens[tokens.length - 2] + "/" + tokens[tokens.length - 1];
      } else {
        throw new IllegalArgumentException("Failed to make path relative. Bad reference: " + path);
      }
    } else if (tokens.length == 3 && (expectedLen == 0 || expectedLen == 3)
        && !path.contains(":")) {
      // we already have a relative path so return it, like 4/t0/F000.rf
      relPath = tokens[0] + "/" + tokens[1] + "/" + tokens[2];
    } else if (tokens.length == 2 && (expectedLen == 0 || expectedLen == 2)
        && !path.contains(":")) {
      // return the last 2 tokens of the relative path, like 4/t0
      relPath = tokens[0] + "/" + tokens[1];
    } else {
      throw new IllegalArgumentException("Failed to make path relative. Bad reference: " + path);
    }

    log.trace("{} -> {} expectedLen = {}", path, relPath, expectedLen);
    return relPath;
  }

  private SortedMap<String,String> makeRelative(Collection<String> candidates) {
    SortedMap<String,String> ret = new TreeMap<>();

    for (String candidate : candidates) {
      String relPath;
      try {
        relPath = makeRelative(candidate, 0);
      } catch (IllegalArgumentException iae) {
        log.warn("Ignoring invalid deletion candidate {}", candidate);
        continue;
      }
      ret.put(relPath, candidate);
    }

    return ret;
  }

  private void removeCandidatesInUse(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws InterruptedException {

    Set<TableId> tableIdsBefore = gce.getCandidateTableIDs();
    Set<TableId> tableIdsSeen = new HashSet<>();
    Iterator<Reference> iter = gce.getReferences().iterator();
    while (iter.hasNext()) {
      Reference ref = iter.next();
      tableIdsSeen.add(ref.getTableId());

      if (ref.isDirectory()) {
        var dirReference = (ReferenceDirectory) ref;
        ServerColumnFamily.validateDirCol(dirReference.getTabletDir());

        String dir = "/" + dirReference.tableId + "/" + dirReference.getTabletDir();

        dir = makeRelative(dir, 2);

        if (candidateMap.remove(dir) != null) {
          log.debug("Candidate was still in use: {}", dir);
        }
      } else {
        String reference = ref.getMetadataEntry();
        if (reference.startsWith("/")) {
          log.debug("Candidate {} has a relative path, prepend tableId {}", reference,
              ref.getTableId());
          reference = "/" + ref.getTableId() + ref.getMetadataEntry();
        } else if (!reference.contains(":") && !reference.startsWith("../")) {
          throw new RuntimeException("Bad file reference " + reference);
        }

        String relativePath = makeRelative(reference, 3);

        // WARNING: This line is EXTREMELY IMPORTANT.
        // You MUST REMOVE candidates that are still in use
        if (candidateMap.remove(relativePath) != null) {
          log.debug("Candidate was still in use: {}", relativePath);
        }

        String dir = relativePath.substring(0, relativePath.lastIndexOf('/'));
        if (candidateMap.remove(dir) != null) {
          log.debug("Candidate was still in use: {}", relativePath);
        }
      }
    }
    Set<TableId> tableIdsAfter = gce.getCandidateTableIDs();
    ensureAllTablesChecked(Collections.unmodifiableSet(tableIdsBefore),
        Collections.unmodifiableSet(tableIdsSeen), Collections.unmodifiableSet(tableIdsAfter));
  }

  private long removeBlipCandidates(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws TableNotFoundException {
    long blipCount = 0;
    boolean checkForBulkProcessingFiles = candidateMap.keySet().stream().anyMatch(
        relativePath -> relativePath.toLowerCase(Locale.ENGLISH).contains(Constants.BULK_PREFIX));

    if (checkForBulkProcessingFiles) {
      try (Stream<String> blipStream = gce.getBlipPaths()) {
        Iterator<String> blipiter = blipStream.iterator();

        // WARNING: This block is IMPORTANT
        // You MUST REMOVE candidates that are in the same folder as a bulk
        // processing flag!

        while (blipiter.hasNext()) {
          blipCount++;
          String blipPath = blipiter.next();
          blipPath = makeRelative(blipPath, 2);

          Iterator<String> tailIter = candidateMap.tailMap(blipPath).keySet().iterator();

          int count = 0;

          while (tailIter.hasNext()) {
            if (tailIter.next().startsWith(blipPath)) {
              count++;
              tailIter.remove();
            } else {
              break;
            }
          }

          if (count > 0) {
            log.debug("Folder has bulk processing flag: {}", blipPath);
          }
        }
      }
    }

    return blipCount;
  }

  @VisibleForTesting
  /**
   * Double check no tables were missed during GC
   */
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

  private void cleanUpDeletedTableDirs(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws InterruptedException, IOException {
    HashSet<TableId> tableIdsWithDeletes = new HashSet<>();

    // find the table ids that had dirs deleted
    for (String delete : candidateMap.keySet()) {
      String[] tokens = delete.split("/");
      if (tokens.length == 2) {
        // its a directory
        TableId tableId = TableId.of(delete.split("/")[0]);
        tableIdsWithDeletes.add(tableId);
      }
    }

    Set<TableId> tableIdsInZookeeper = gce.getTableIDs().keySet();

    tableIdsWithDeletes.removeAll(tableIdsInZookeeper);

    // tableIdsWithDeletes should now contain the set of deleted tables that had dirs deleted

    for (TableId delTableId : tableIdsWithDeletes) {
      gce.deleteTableDirIfEmpty(delTableId);
    }
  }

  private long confirmDeletesTrace(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws InterruptedException, TableNotFoundException {
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

  private void deleteConfirmedCandidates(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap)
      throws InterruptedException, IOException, TableNotFoundException {
    Span deleteSpan = TraceUtil.startSpan(this.getClass(), "deleteFiles");
    try (Scope deleteScope = deleteSpan.makeCurrent()) {
      gce.deleteConfirmedCandidates(candidateMap);
    } catch (Exception e) {
      TraceUtil.setException(deleteSpan, e, true);
      throw e;
    } finally {
      deleteSpan.end();
    }

    cleanUpDeletedTableDirs(gce, candidateMap);
  }

  public long collect(GarbageCollectionEnvironment gce)
      throws InterruptedException, TableNotFoundException, IOException {

    Iterator<String> candidatesIter = gce.getCandidates();
    long totalBlips = 0;

    while (candidatesIter.hasNext()) {
      List<String> batchOfCandidates;
      Span candidatesSpan = TraceUtil.startSpan(this.getClass(), "getCandidates");
      try (Scope candidatesScope = candidatesSpan.makeCurrent()) {
        batchOfCandidates = gce.readCandidatesThatFitInMemory(candidatesIter);
      } catch (Exception e) {
        TraceUtil.setException(candidatesSpan, e, true);
        throw e;
      } finally {
        candidatesSpan.end();
      }
      totalBlips = deleteBatch(gce, batchOfCandidates);
    }
    return totalBlips;
  }

  /**
   * Given a sub-list of possible deletion candidates, process and remove valid deletion candidates.
   */
  private long deleteBatch(GarbageCollectionEnvironment gce, List<String> currentBatch)
      throws InterruptedException, TableNotFoundException, IOException {

    long origSize = currentBatch.size();
    gce.incrementCandidatesStat(origSize);

    SortedMap<String,String> candidateMap = makeRelative(currentBatch);

    long blips = confirmDeletesTrace(gce, candidateMap);
    gce.incrementInUseStat(origSize - candidateMap.size());

    deleteConfirmedCandidates(gce, candidateMap);

    return blips;
  }

}
