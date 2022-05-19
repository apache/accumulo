/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.Reference;
import org.apache.accumulo.core.metadata.RelativeTabletDirectory;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

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
    if (relPath.startsWith("../"))
      relPath = relPath.substring(3);

    // remove trailing slash
    while (relPath.endsWith("/"))
      relPath = relPath.substring(0, relPath.length() - 1);

    // remove beginning slash
    while (relPath.startsWith("/"))
      relPath = relPath.substring(1);

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
      SortedMap<String,String> candidateMap) {
    Iterator<Reference> iter = gce.getReferences().iterator();
    while (iter.hasNext()) {
      Reference ref = iter.next();

      if (ref instanceof RelativeTabletDirectory) {
        var dirReference = (RelativeTabletDirectory) ref;
        ServerColumnFamily.validateDirCol(dirReference.tabletDir);

        String dir = "/" + dirReference.tableId + "/" + dirReference.tabletDir;

        dir = makeRelative(dir, 2);

        if (candidateMap.remove(dir) != null)
          log.debug("Candidate was still in use: {}", dir);
      } else {
        String reference = ref.metadataEntry;
        if (reference.startsWith("/")) {
          log.debug("Candidate {} has a relative path, prepend tableId {}", reference, ref.tableId);
          reference = "/" + ref.tableId + ref.metadataEntry;
        } else if (!reference.contains(":") && !reference.startsWith("../")) {
          throw new RuntimeException("Bad file reference " + reference);
        }

        String relativePath = makeRelative(reference, 3);

        // WARNING: This line is EXTREMELY IMPORTANT.
        // You MUST REMOVE candidates that are still in use
        if (candidateMap.remove(relativePath) != null)
          log.debug("Candidate was still in use: {}", relativePath);

        String dir = relativePath.substring(0, relativePath.lastIndexOf('/'));
        if (candidateMap.remove(dir) != null)
          log.debug("Candidate was still in use: {}", relativePath);
      }
    }
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

  protected void confirmDeletesFromReplication(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) {
    var replicationNeededIterator = gce.getReplicationNeededIterator();
    var candidateMapIterator = candidateMap.entrySet().iterator();

    PeekingIterator<Entry<String,Status>> pendingReplication =
        Iterators.peekingIterator(replicationNeededIterator);
    PeekingIterator<Entry<String,String>> candidates =
        Iterators.peekingIterator(candidateMapIterator);
    while (pendingReplication.hasNext() && candidates.hasNext()) {
      Entry<String,Status> pendingReplica = pendingReplication.peek();
      Entry<String,String> candidate = candidates.peek();

      String filePendingReplication = pendingReplica.getKey();
      String fullPathCandidate = candidate.getValue();

      int comparison = filePendingReplication.compareTo(fullPathCandidate);
      if (comparison < 0) {
        pendingReplication.next();
      } else if (comparison > 1) {
        candidates.next();
      } else {
        // We want to advance both, and try to delete the candidate if we can
        candidates.next();
        pendingReplication.next();

        // We cannot delete a file if it is still needed for replication
        @SuppressWarnings("deprecation")
        boolean safeToRemove = org.apache.accumulo.server.replication.StatusUtil
            .isSafeForRemoval(pendingReplica.getValue());
        if (!safeToRemove) {
          // If it must be replicated, we must remove it from the candidate set to prevent deletion
          candidates.remove();
        }
      }
    }
  }

  private void cleanUpDeletedTableDirs(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws IOException {
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

    Set<TableId> tableIdsInZookeeper = gce.getTableIDs();

    tableIdsWithDeletes.removeAll(tableIdsInZookeeper);

    // tableIdsWithDeletes should now contain the set of deleted tables that had dirs deleted

    for (TableId delTableId : tableIdsWithDeletes) {
      gce.deleteTableDirIfEmpty(delTableId);
    }
  }

  private long confirmDeletesTrace(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws TableNotFoundException {
    long blips = 0;
    Span confirmDeletesSpan = TraceUtil.startSpan(this.getClass(), "confirmDeletes");
    try (Scope scope = confirmDeletesSpan.makeCurrent()) {
      blips = removeBlipCandidates(gce, candidateMap);
      removeCandidatesInUse(gce, candidateMap);
      confirmDeletesFromReplication(gce, candidateMap);
    } catch (Exception e) {
      TraceUtil.setException(confirmDeletesSpan, e, true);
      throw e;
    } finally {
      confirmDeletesSpan.end();
    }
    return blips;
  }

  private void deleteConfirmedCandidates(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws IOException, TableNotFoundException {
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

  public long collect(GarbageCollectionEnvironment gce) throws TableNotFoundException, IOException {

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
      throws TableNotFoundException, IOException {

    long origSize = currentBatch.size();
    gce.incrementCandidatesStat(origSize);

    SortedMap<String,String> candidateMap = makeRelative(currentBatch);

    long blips = confirmDeletesTrace(gce, candidateMap);
    gce.incrementInUseStat(origSize - candidateMap.size());

    deleteConfirmedCandidates(gce, candidateMap);

    return blips;
  }

}
