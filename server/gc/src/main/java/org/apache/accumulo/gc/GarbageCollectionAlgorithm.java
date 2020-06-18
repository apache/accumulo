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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.gc.GarbageCollectionEnvironment.Reference;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class GarbageCollectionAlgorithm {

  private static final Logger log = LoggerFactory.getLogger(GarbageCollectionAlgorithm.class);

  private String makeRelative(String path, int expectedLen) {
    String relPath = path;

    if (relPath.startsWith("../"))
      relPath = relPath.substring(3);

    while (relPath.endsWith("/"))
      relPath = relPath.substring(0, relPath.length() - 1);

    while (relPath.startsWith("/"))
      relPath = relPath.substring(1);

    String[] tokens = relPath.split("/");

    // handle paths like a//b///c
    boolean containsEmpty = false;
    for (String token : tokens) {
      if (token.equals("")) {
        containsEmpty = true;
        break;
      }
    }

    if (containsEmpty) {
      ArrayList<String> tmp = new ArrayList<>();
      for (String token : tokens) {
        if (!token.equals("")) {
          tmp.add(token);
        }
      }

      tokens = tmp.toArray(new String[tmp.size()]);
    }

    if (tokens.length > 3 && path.contains(":")) {
      if (tokens[tokens.length - 4].equals(ServerConstants.TABLE_DIR)
          && (expectedLen == 0 || expectedLen == 3)) {
        relPath = tokens[tokens.length - 3] + "/" + tokens[tokens.length - 2] + "/"
            + tokens[tokens.length - 1];
      } else if (tokens[tokens.length - 3].equals(ServerConstants.TABLE_DIR)
          && (expectedLen == 0 || expectedLen == 2)) {
        relPath = tokens[tokens.length - 2] + "/" + tokens[tokens.length - 1];
      } else {
        throw new IllegalArgumentException(path);
      }
    } else if (tokens.length == 3 && (expectedLen == 0 || expectedLen == 3)
        && !path.contains(":")) {
      relPath = tokens[0] + "/" + tokens[1] + "/" + tokens[2];
    } else if (tokens.length == 2 && (expectedLen == 0 || expectedLen == 2)
        && !path.contains(":")) {
      relPath = tokens[0] + "/" + tokens[1];
    } else {
      throw new IllegalArgumentException(path);
    }

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

  private void confirmDeletes(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws TableNotFoundException {
    boolean checkForBulkProcessingFiles = false;
    Iterator<String> relativePaths = candidateMap.keySet().iterator();
    while (!checkForBulkProcessingFiles && relativePaths.hasNext())
      checkForBulkProcessingFiles |=
          relativePaths.next().toLowerCase(Locale.ENGLISH).contains(Constants.BULK_PREFIX);

    if (checkForBulkProcessingFiles) {
      Iterator<String> blipiter = gce.getBlipIterator();

      // WARNING: This block is IMPORTANT
      // You MUST REMOVE candidates that are in the same folder as a bulk
      // processing flag!

      while (blipiter.hasNext()) {
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

        if (count > 0)
          log.debug("Folder has bulk processing flag: {}", blipPath);
      }

    }

    Iterator<Reference> iter = gce.getReferences().iterator();
    while (iter.hasNext()) {
      Reference ref = iter.next();

      if (ref.isDir) {
        String tableID = ref.id.toString();
        String dirName = ref.ref;
        ServerColumnFamily.validateDirCol(dirName);

        String dir = "/" + tableID + "/" + dirName;

        dir = makeRelative(dir, 2);

        if (candidateMap.remove(dir) != null)
          log.debug("Candidate was still in use: {}", dir);
      } else {

        String reference = ref.ref;
        if (reference.startsWith("/")) {
          reference = "/" + ref.id + reference;
        } else if (!reference.contains(":") && !reference.startsWith("../")) {
          throw new RuntimeException("Bad file reference " + reference);
        }

        reference = makeRelative(reference, 3);

        // WARNING: This line is EXTREMELY IMPORTANT.
        // You MUST REMOVE candidates that are still in use
        if (candidateMap.remove(reference) != null)
          log.debug("Candidate was still in use: {}", reference);

        String dir = reference.substring(0, reference.lastIndexOf('/'));
        if (candidateMap.remove(dir) != null)
          log.debug("Candidate was still in use: {}", reference);

      }
    }

    confirmDeletesFromReplication(gce.getReplicationNeededIterator(),
        candidateMap.entrySet().iterator());
  }

  protected void confirmDeletesFromReplication(
      Iterator<Entry<String,Status>> replicationNeededIterator,
      Iterator<Entry<String,String>> candidateMapIterator) {
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
        if (!StatusUtil.isSafeForRemoval(pendingReplica.getValue())) {
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

  private boolean getCandidates(GarbageCollectionEnvironment gce, String lastCandidate,
      List<String> candidates) throws TableNotFoundException {
    try (TraceScope candidatesSpan = Trace.startSpan("getCandidates")) {
      return gce.getCandidates(lastCandidate, candidates);
    }
  }

  private void confirmDeletesTrace(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws TableNotFoundException {
    try (TraceScope confirmDeletesSpan = Trace.startSpan("confirmDeletes")) {
      confirmDeletes(gce, candidateMap);
    }
  }

  private void deleteConfirmed(GarbageCollectionEnvironment gce,
      SortedMap<String,String> candidateMap) throws IOException, TableNotFoundException {
    try (TraceScope deleteSpan = Trace.startSpan("deleteFiles")) {
      gce.delete(candidateMap);
    }

    cleanUpDeletedTableDirs(gce, candidateMap);
  }

  public void collect(GarbageCollectionEnvironment gce) throws TableNotFoundException, IOException {

    String lastCandidate = "";

    boolean outOfMemory = true;
    while (outOfMemory) {
      List<String> candidates = new ArrayList<>();

      outOfMemory = getCandidates(gce, lastCandidate, candidates);

      if (candidates.isEmpty())
        break;
      else
        lastCandidate = candidates.get(candidates.size() - 1);

      long origSize = candidates.size();
      gce.incrementCandidatesStat(origSize);

      SortedMap<String,String> candidateMap = makeRelative(candidates);

      confirmDeletesTrace(gce, candidateMap);
      gce.incrementInUseStat(origSize - candidateMap.size());

      deleteConfirmed(gce, candidateMap);
    }
  }
}
