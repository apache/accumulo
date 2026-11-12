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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.metadata.schema.Ample.GcCandidateType;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class GarbageCollectionAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(GarbageCollectionAlgorithm.class);

  /**
   * Normalizes a candidate, directory, or blip path to a table-relative GC key. Absolute file paths
   * retain their full URI identity in {@link #fileCandidateKey(String)}.
   */
  private String makeRelative(String path, int expectedLen) {
    String relative = tryMakeRelative(path, expectedLen);
    if (relative == null) {
      throw new IllegalArgumentException("Failed to make path relative. Bad reference: " + path);
    }
    return relative;
  }

  // Returns null when the path does not match the expected Accumulo table path layout.
  private String tryMakeRelative(String path, int expectedLen) {
    // remove prefixed old relative path
    int position = path.startsWith("../") ? 3 : 0;
    int componentCount = 0;
    int start0 = 0;
    int end0 = 0;
    int start1 = 0;
    int end1 = 0;
    int start2 = 0;
    int end2 = 0;
    int start3 = 0;
    int end3 = 0;
    boolean hasColon = false;

    // Scan once, ignoring empty components and retaining only the last four ranges.
    while (position < path.length()) {
      while (position < path.length() && path.charAt(position) == '/') {
        position++;
      }
      if (position == path.length()) {
        break;
      }

      int componentStart = position;
      while (position < path.length() && path.charAt(position) != '/') {
        if (path.charAt(position) == ':') {
          hasColon = true;
        }
        position++;
      }
      int componentEnd = position;

      start0 = start1;
      end0 = end1;
      start1 = start2;
      end1 = end2;
      start2 = start3;
      end2 = end3;
      start3 = componentStart;
      end3 = componentEnd;
      componentCount++;
    }

    int outputStart1;
    int outputEnd1;
    int outputStart2;
    int outputEnd2;
    int outputStart3 = 0;
    int outputEnd3 = 0;
    int outputComponents;

    if (componentCount > 3 && hasColon) {
      // full file path like hdfs://foo:6000/accumulo/tables/4/t0/F000.rf
      int tablesLength = end0 - start0;
      if (tablesLength == Constants.TABLE_DIR.length()
          && path.regionMatches(start0, Constants.TABLE_DIR, 0, tablesLength)
          && (expectedLen == 0 || expectedLen == 3)) {
        // return the last 3 components after tables, like 4/t0/F000.rf
        outputStart1 = start1;
        outputEnd1 = end1;
        outputStart2 = start2;
        outputEnd2 = end2;
        outputStart3 = start3;
        outputEnd3 = end3;
        outputComponents = 3;
      } else {
        int directoryTablesLength = end1 - start1;
        if (directoryTablesLength == Constants.TABLE_DIR.length()
            && path.regionMatches(start1, Constants.TABLE_DIR, 0, directoryTablesLength)
            && (expectedLen == 0 || expectedLen == 2)) {
          // return the last 2 components after tables, like 4/t0
          outputStart1 = start2;
          outputEnd1 = end2;
          outputStart2 = start3;
          outputEnd2 = end3;
          outputComponents = 2;
        } else {
          return null;
        }
      }
    } else if (componentCount == 3 && (expectedLen == 0 || expectedLen == 3) && !hasColon) {
      // we already have a relative path so return it, like 4/t0/F000.rf
      outputStart1 = start1;
      outputEnd1 = end1;
      outputStart2 = start2;
      outputEnd2 = end2;
      outputStart3 = start3;
      outputEnd3 = end3;
      outputComponents = 3;
    } else if (componentCount == 2 && (expectedLen == 0 || expectedLen == 2) && !hasColon) {
      // return the last 2 components of the relative path, like 4/t0
      outputStart1 = start2;
      outputEnd1 = end2;
      outputStart2 = start3;
      outputEnd2 = end3;
      outputComponents = 2;
    } else {
      return null;
    }

    String relPath;
    boolean contiguous = outputEnd1 + 1 == outputStart2 && path.charAt(outputEnd1) == '/';
    if (outputComponents == 2) {
      if (contiguous) {
        relPath = path.substring(outputStart1, outputEnd2);
      } else {
        relPath = new StringBuilder(outputEnd1 - outputStart1 + outputEnd2 - outputStart2 + 1)
            .append(path, outputStart1, outputEnd1).append('/')
            .append(path, outputStart2, outputEnd2).toString();
      }
    } else if (contiguous && outputEnd2 + 1 == outputStart3 && path.charAt(outputEnd2) == '/') {
      relPath = path.substring(outputStart1, outputEnd3);
    } else {
      relPath = new StringBuilder(
          outputEnd1 - outputStart1 + outputEnd2 - outputStart2 + outputEnd3 - outputStart3 + 2)
          .append(path, outputStart1, outputEnd1).append('/').append(path, outputStart2, outputEnd2)
          .append('/').append(path, outputStart3, outputEnd3).toString();
    }

    log.trace("{} -> {} expectedLen = {}", path, relPath, expectedLen);
    return relPath;
  }

  private String makeCandidateKey(String path) {
    String relative = tryMakeRelative(path, 0);
    if (relative != null && SimpleGarbageCollector.isDir(relative)) {
      return directoryCandidateKey(relative);
    }

    return fileCandidateKey(path);
  }

  private String fileCandidateKey(String path) {
    if (path.endsWith("/")) {
      throw new IllegalArgumentException("GC file candidate is a directory: " + path);
    }
    Path candidatePath = new Path(path);
    String normalizedPath = candidatePath.toString();
    if (candidatePath.toUri().getScheme() != null) {
      if (candidatePath.getName().isEmpty()) {
        throw new IllegalArgumentException("GC file candidate does not identify a file: " + path);
      }
      String relative = tryMakeRelative(normalizedPath, 3);
      if (relative != null) {
        // Keep managed files ordered by their relative tablet path so directory range lookups can
        // find every volume. The normalized URI suffix preserves identity across volumes.
        return relative + "|" + normalizedPath;
      }
      return normalizedPath;
    }

    // Relative paths are retained for compatibility with older metadata entries. Once paths are
    // absolute, full URI identity is required so files at distinct locations cannot alias.
    String relative = tryMakeRelative(path, 3);
    if (relative == null) {
      throw new IllegalArgumentException("Invalid relative GC file candidate: " + path);
    }
    return relative;
  }

  private String directoryCandidateKey(String relativePath) {
    String normalized = relativePath;
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized + "/";
  }

  static boolean isDir(String candidateKey) {
    return candidateKey != null && !candidateKey.isEmpty()
        && candidateKey.charAt(candidateKey.length() - 1) == '/';
  }

  private String tabletDirectoryKey(String filePath) {
    String relativeFile = tryMakeRelative(filePath, 3);
    if (relativeFile == null) {
      // External files are not in an Accumulo tablet directory and have no directory candidate to
      // suppress.
      return null;
    }
    return directoryCandidateKey(relativeFile.substring(0, relativeFile.lastIndexOf('/')));
  }

  private SortedMap<String,GcCandidate> makeCandidateMap(Collection<GcCandidate> candidates) {
    SortedMap<String,GcCandidate> ret = new TreeMap<>();

    for (GcCandidate candidate : candidates) {
      try {
        ret.put(makeCandidateKey(candidate.getPath()), candidate);
      } catch (IllegalArgumentException e) {
        log.warn("Ignoring invalid deletion candidate {}", candidate);
      }
    }

    return ret;
  }

  private void removeCandidatesInUse(GarbageCollectionEnvironment gce,
      SortedMap<String,GcCandidate> candidateMap) throws InterruptedException {

    List<GcCandidate> candidateEntriesToBeDeleted = new ArrayList<>();
    Set<TableId> tableIdsBefore = gce.getCandidateTableIDs();
    Set<TableId> tableIdsSeen = new HashSet<>(tableIdsBefore.size());
    try (Stream<Reference> references = gce.getReferences()) {
      references.forEach(ref -> {

        try {
          tableIdsSeen.add(ref.getTableId());
        } catch (UnsupportedOperationException uoe) {
          // ScanServerRefTabletFile's don't have table ids
        }

        if (ref.isDirectory()) {
          ServerColumnFamily.validateDirCol(ref.getMetadataPath());

          String dir = "/" + ref.getTableId() + "/" + ref.getMetadataPath();

          dir = directoryCandidateKey(makeRelative(dir, 2));

          GcCandidate gcTemp = candidateMap.remove(dir);
          if (gcTemp != null) {
            log.debug("Directory Candidate was still in use by dir ref: {}", dir);
            // Do not add dir candidates to candidateEntriesToBeDeleted as they are only created
            // once.
          }
        } else {
          String reference = ref.getMetadataPath();
          Path refPath = new Path(reference);
          if (!refPath.isAbsolute() || refPath.toUri().getScheme() == null) {
            throw new IllegalArgumentException(
                "File reference must be an absolute URI: " + reference);
          }

          List<GcCandidate> inUseCandidates = new ArrayList<>();
          GcCandidate gcTemp = candidateMap.remove(fileCandidateKey(reference));
          if (gcTemp != null) {
            inUseCandidates.add(gcTemp);
          }

          // Legacy relative candidates do not identify a filesystem location. Conservatively
          // protect them by suffix, while absolute candidates always require exact URI identity.
          String relativePath = tryMakeRelative(refPath.toString(), 3);
          if (relativePath != null) {
            GcCandidate relativeCandidate = candidateMap.remove(relativePath);
            if (relativeCandidate != null) {
              inUseCandidates.add(relativeCandidate);
            }
          }

          for (GcCandidate inUseCandidate : inUseCandidates) {
            log.debug("File Candidate was still in use: {}", reference);
            // Prevent deletion of candidates that are still in use by scans, because they won't be
            // recreated once the scan is finished.
            if (!ref.isScan()) {
              candidateEntriesToBeDeleted.add(inUseCandidate);
            }
          }

          String dirKey = tabletDirectoryKey(reference);
          GcCandidate gcT = dirKey == null ? null : candidateMap.remove(dirKey);
          if (gcT != null) {
            log.debug("Directory Candidate was still in use by file ref: {}", reference);
            // Do not add dir candidates to candidateEntriesToBeDeleted as they are only created
            // once.
          }
        }
      });
    }
    Set<TableId> tableIdsAfter = gce.getCandidateTableIDs();
    ensureAllTablesChecked(Collections.unmodifiableSet(tableIdsBefore),
        Collections.unmodifiableSet(tableIdsSeen), Collections.unmodifiableSet(tableIdsAfter));
    gce.deleteGcCandidates(candidateEntriesToBeDeleted, GcCandidateType.INUSE);
  }

  private long removeBlipCandidates(GarbageCollectionEnvironment gce,
      SortedMap<String,GcCandidate> candidateMap) throws TableNotFoundException {
    long blipCount = 0;
    boolean checkForBulkProcessingFiles = false;
    for (GcCandidate candidate : candidateMap.values()) {
      if (containsBulkPrefix(candidate.getPath())) {
        checkForBulkProcessingFiles = true;
        break;
      }
    }

    if (checkForBulkProcessingFiles) {
      try (Stream<String> blipStream = gce.getBlipPaths()) {
        Iterator<String> blipiter = blipStream.iterator();

        // WARNING: This block is IMPORTANT
        // You MUST REMOVE candidates that are in the same folder as a bulk
        // processing flag!

        while (blipiter.hasNext()) {
          blipCount++;
          String blipPath = blipiter.next();
          Path blip = new Path(blipPath);
          String normalizedBlipPath = blip.toString();
          boolean absoluteBlip = blip.toUri().getScheme() != null;
          String relativeBlip = tryMakeRelative(normalizedBlipPath, 2);

          int count = 0;
          if (relativeBlip != null) {
            String relativePrefix = directoryCandidateKey(relativeBlip);
            Iterator<Entry<String,GcCandidate>> relativeIter =
                candidateMap.tailMap(relativePrefix).entrySet().iterator();
            while (relativeIter.hasNext()) {
              Entry<String,GcCandidate> candidateEntry = relativeIter.next();
              if (!candidateEntry.getKey().startsWith(relativePrefix)) {
                break;
              }

              if (!absoluteBlip) {
                count++;
                relativeIter.remove();
              } else {
                Path candidatePath = new Path(candidateEntry.getValue().getPath());
                if (candidatePath.toUri().getScheme() == null
                    || isDescendantOrSame(blip, candidatePath)) {
                  count++;
                  relativeIter.remove();
                }
              }
            }
          }

          if (absoluteBlip) {
            String absolutePrefix =
                normalizedBlipPath.endsWith("/") ? normalizedBlipPath : normalizedBlipPath + "/";
            Iterator<Entry<String,GcCandidate>> absoluteIter =
                candidateMap.tailMap(absolutePrefix).entrySet().iterator();
            while (absoluteIter.hasNext()) {
              Entry<String,GcCandidate> candidateEntry = absoluteIter.next();
              if (!candidateEntry.getKey().startsWith(absolutePrefix)) {
                break;
              }
              count++;
              absoluteIter.remove();
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

  private static boolean isDescendantOrSame(Path ancestor, Path candidate) {
    Path current = candidate;
    while (current != null) {
      if (current.equals(ancestor)) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }

  private boolean containsBulkPrefix(String path) {
    int prefixLength = Constants.BULK_PREFIX.length();
    for (int i = 0; i <= path.length() - prefixLength; i++) {
      if (path.regionMatches(true, i, Constants.BULK_PREFIX, 0, prefixLength)) {
        return true;
      }
    }
    return false;
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

  private void cleanUpDeletedTableDirs(GarbageCollectionEnvironment gce,
      SortedMap<String,GcCandidate> candidateMap) throws InterruptedException, IOException {
    HashSet<TableId> tableIdsWithDeletes = new HashSet<>();

    // find the table ids that had dirs deleted
    for (String delete : candidateMap.keySet()) {
      if (!isDir(delete)) {
        continue;
      }
      int firstSlash = delete.indexOf('/');
      if (firstSlash > 0 && firstSlash < delete.length() - 2
          && delete.indexOf('/', firstSlash + 1) == delete.length() - 1) {
        // its a directory
        TableId tableId = TableId.of(delete.substring(0, firstSlash));
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
      SortedMap<String,GcCandidate> candidateMap)
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

  private void deleteConfirmedCandidates(GarbageCollectionEnvironment gce,
      SortedMap<String,GcCandidate> candidateMap)
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

    SortedMap<String,GcCandidate> candidateMap = makeCandidateMap(currentBatch);

    long blips = confirmDeletesTrace(gce, candidateMap);
    gce.incrementInUseStat(origSize - candidateMap.size());

    deleteConfirmedCandidates(gce, candidateMap);

    return blips;
  }
}
