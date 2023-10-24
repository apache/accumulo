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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.gc.ReferenceDirectory;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.GcCandidateType;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.junit.jupiter.api.Test;

public class GarbageCollectionTest {

  static class TestGCE implements GarbageCollectionEnvironment {
    TreeSet<GcCandidate> candidates = new TreeSet<>();
    TreeMap<GcCandidate,GcCandidateType> deletedCandidates = new TreeMap<>();
    ArrayList<String> blips = new ArrayList<>();
    Map<String,Reference> references = new TreeMap<>();
    HashSet<TableId> tableIds = new HashSet<>();

    ArrayList<GcCandidate> fileDeletions = new ArrayList<>();
    ArrayList<TableId> tablesDirsToDelete = new ArrayList<>();
    TreeMap<String,Status> filesToReplicate = new TreeMap<>();
    boolean deleteInUseRefs = false;

    private long timestamp = 0L;

    private final Ample.DataLevel level;

    TestGCE(Ample.DataLevel level) {
      this.level = level;
    }

    TestGCE() {
      this.level = Ample.DataLevel.USER;
    }

    public GcCandidate addCandidate(String s) {
      var candidate = new GcCandidate(s, timestamp);
      this.candidates.add(candidate);
      this.timestamp = timestamp + 1;
      return candidate;
    }

    public Long getTimestamp() {
      return timestamp;
    }

    @Override
    public Iterator<GcCandidate> getCandidates() throws TableNotFoundException {
      return List.copyOf(candidates).iterator();
    }

    @Override
    public boolean canRemoveInUseCandidates() {
      return deleteInUseRefs;
    }

    @Override
    public List<GcCandidate> readCandidatesThatFitInMemory(Iterator<GcCandidate> candidatesIter) {
      List<GcCandidate> candidatesBatch = new ArrayList<>();
      while (candidatesIter.hasNext() && candidatesBatch.size() < 3) {
        candidatesBatch.add(candidatesIter.next());
      }
      return candidatesBatch;
    }

    @Override
    public Stream<String> getBlipPaths() {
      return blips.stream();
    }

    @Override
    public Stream<Reference> getReferences() {
      return references.values().stream();
    }

    @Override
    public void deleteGcCandidates(Collection<GcCandidate> refCandidates, GcCandidateType type) {
      // Mimic ServerAmpleImpl behavior for root InUse Candidates
      if (type.equals(GcCandidateType.INUSE) && this.level.equals(Ample.DataLevel.ROOT)) {
        // Since there is only a single root tablet, supporting INUSE candidate deletions would add
        // additional code complexity without any substantial benefit.
        // Therefore, deletion of root INUSE candidates is not supported.
        return;
      }
      refCandidates.forEach(gcCandidate -> deletedCandidates.put(gcCandidate, type));
      this.candidates.removeAll(refCandidates);
    }

    @Override
    public Map<TableId,TableState> getTableIDs() {
      HashMap<TableId,TableState> results = new HashMap<>();
      tableIds.forEach((t) -> results.put(t, TableState.ONLINE));
      return results;
    }

    @Override
    public void deleteConfirmedCandidates(SortedMap<String,GcCandidate> candidateMap) {
      fileDeletions.addAll(candidateMap.values());
      this.candidates.removeAll(candidateMap.values());
    }

    @Override
    public void deleteTableDirIfEmpty(TableId tableID) {
      tablesDirsToDelete.add(tableID);
    }

    public void addFileReference(String tableId, String endRow, String file) {
      TableId tid = TableId.of(tableId);
      references.put(tableId + ":" + endRow + ":" + file, ReferenceFile.forFile(tid, file));
      tableIds.add(tid);
    }

    public void removeFileReference(String tableId, String endRow, String file) {
      references.remove(tableId + ":" + endRow + ":" + file);
      removeLastTableIdRef(TableId.of(tableId));
    }

    public void addDirReference(String tableId, String endRow, String dir) {
      TableId tid = TableId.of(tableId);
      references.put(tableId + ":" + endRow, new ReferenceDirectory(tid, dir));
      tableIds.add(tid);
    }

    public void removeDirReference(String tableId, String endRow) {
      references.remove(tableId + ":" + endRow);
      removeLastTableIdRef(TableId.of(tableId));
    }

    public void addScanReference(String tableId, String endRow, String scan) {
      TableId tid = TableId.of(tableId);
      references.put(tableId + ":" + endRow + ":scan:" + scan, ReferenceFile.forScan(tid, scan));
      tableIds.add(tid);
    }

    public void removeScanReference(String tableId, String endRow, String scan) {
      references.remove(tableId + ":" + endRow + ":scan:" + scan);
      removeLastTableIdRef(TableId.of(tableId));
    }

    /*
     * this is to be called from removeDirReference or removeFileReference.
     *
     * If you just removed the last reference to a table, we need to remove it from the tableIds in
     * zookeeper
     */
    private void removeLastTableIdRef(TableId tableId) {
      boolean inUse = references.keySet().stream().map(k -> k.substring(0, k.indexOf(':')))
          .anyMatch(tid -> tableId.canonical().equals(tid));
      if (!inUse) {
        assertTrue(tableIds.remove(tableId));
      }
    }

    @Override
    public void incrementCandidatesStat(long i) {}

    @Override
    public void incrementInUseStat(long i) {}

    @Override
    public Iterator<Entry<String,Status>> getReplicationNeededIterator() {
      return filesToReplicate.entrySet().iterator();
    }

    @Override
    public Set<TableId> getCandidateTableIDs() {
      if (level == Ample.DataLevel.ROOT) {
        return Set.of(RootTable.ID);
      } else if (level == Ample.DataLevel.METADATA) {
        return Collections.singleton(MetadataTable.ID);
      } else if (level == Ample.DataLevel.USER) {
        Set<TableId> tableIds = new HashSet<>();
        getTableIDs().forEach((k, v) -> {
          if (v == TableState.ONLINE || v == TableState.OFFLINE) {
            // Don't return tables that are NEW, DELETING, or in an
            // UNKNOWN state.
            tableIds.add(k);
          }
        });
        tableIds.remove(MetadataTable.ID);
        tableIds.remove(RootTable.ID);
        return tableIds;
      } else {
        throw new IllegalArgumentException("unknown level " + level);
      }
    }
  }

  private void assertFileDeleted(TestGCE gce, GcCandidate... candidates) {
    for (GcCandidate candidate : candidates) {
      assertTrue(gce.fileDeletions.remove(candidate));
    }

    assertEquals(0, gce.fileDeletions.size(), "Deletes not empty: " + gce.fileDeletions);
  }

  private void assertNoCandidatesRemoved(TestGCE gce) {
    assertEquals(0, gce.deletedCandidates.size(),
        "Deleted Candidates not empty: " + gce.deleteInUseRefs);
  }

  private void assertCandidateRemoved(TestGCE gce, GcCandidateType gcCandidateType,
      GcCandidate... gcCandidates) {
    for (GcCandidate gcCandidate : gcCandidates) {
      assertEquals(gcCandidateType, gce.deletedCandidates.remove(gcCandidate));
    }
    assertEquals(0, gce.deletedCandidates.size(),
        "Deleted Candidates not empty: " + gce.deleteInUseRefs);
  }

  // This test was created to help track down a ConcurrentModificationException error that was
  // occurring with the unit tests once the GC was refactored to use a single iterator for the
  // collect process. This was a minimal test case that would cause the exception to occur.
  @Test
  public void minimalDelete() throws Exception {
    TestGCE gce = new TestGCE();

    gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    var candidate = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/6/t0/F006.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/6/t0/F006.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gca.collect(gce);

    assertFileDeleted(gce, candidate);
  }

  @Test
  public void testBasic() throws Exception {
    TestGCE gce = new TestGCE();

    var candOne = gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    var candTwo = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0//F002.rf");
    gce.addFileReference("5", null, "hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    gca.collect(gce);
    assertFileDeleted(gce);

    // Remove the reference to this flush file, run the GC which should not trim it from the
    // candidates, and assert that it's gone
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candOne);

    // Removing a reference to a file that wasn't in the candidates should do nothing
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce);

    // Remove the reference to a file in the candidates should cause it to be removed
    gce.removeFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candTwo);

    // Adding more candidates which do not have references should be removed
    var candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F003.rf");
    var candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F004.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candThree, candFour);

  }

  /*
   * Additional test with more candidates. Also, not a multiple of 3 as the test above. Since the
   * unit tests always return 3 candidates in a batch, some edge cases could be missed if that was
   * always the case.
   */
  @Test
  public void testBasic2() throws Exception {
    TestGCE gce = new TestGCE();

    GcCandidate[] toBeRemoved = new GcCandidate[20];

    var candOne = gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    var candTwo = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    int counter = 0;
    // items to be removed from candidates
    String[] paths = {"hdfs://foo.com:6000/accumulo/tables/5/t0/F001.rf",

        "hdfs://foo:6000/accumulo/tables/5/t0/F000.rf",
        "hdfs://foo.com:6000/accumulo/tables/6/t1/F005.rf",
        "hdfs://foo:6000/accumulo/tables/6/t0/F000.rf",
        "hdfs://foo.com:6000/accumulo/tables/6/t0/F001.rf",
        "hdfs://foo.com:6000/accumulo/tables/7/t0/F005.rf",
        "hdfs://foo:6000/accumulo/tables/7/t0/F000.rf",
        "hdfs://foo.com:6000/accumulo/tables/7/t0/F001.rf",
        "hdfs://foo.com:6000/accumulo/tables/8/t0/F005.rf",
        "hdfs://foo:6000/accumulo/tables/8/t0/F000.rf",
        "hdfs://foo.com:6000/accumulo/tables/8/t0/F001.rf",
        "hdfs://foo.com:6000/accumulo/tables/9/t0/F005.rf",
        "hdfs://foo:6000/accumulo/tables/9/t0/F000.rf",
        "hdfs://foo.com:6000/accumulo/tables/9/t0/F001.rf",
        "hdfs://foo.com:6000/accumulo/tables/10/t0/F005.rf",
        "hdfs://foo:6000/accumulo/tables/10/t0/F000.rf",
        "hdfs://foo.com:6000/accumulo/tables/10/t0/F001.rf",
        "hdfs://foo.com:6000/accumulo/tables/11/t0/F005.rf",
        "hdfs://foo:6000/accumulo/tables/11/t0/F000.rf",
        "hdfs://foo.com:6000/accumulo/tables/11/t0/F001.rf"};
    for (String path : paths) {
      toBeRemoved[counter] = gce.addCandidate(path);
      counter++;
    }

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0//F002.rf");
    gce.addFileReference("5", null, "hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    gca.collect(gce);
    assertFileDeleted(gce, toBeRemoved);

    // Remove the reference to this flush file, run the GC which should not trim it from the
    // candidates, and assert that it's gone
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candOne);

    // Removing a reference to a file that wasn't in the candidates should do nothing
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce);

    // Remove the reference to a file in the candidates should cause it to be removed
    gce.removeFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candTwo);

    // Adding more candidates which do no have references should be removed
    var candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F003.rf");
    var candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F004.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candThree, candFour);
  }

  /**
   * Tests valid file paths that have empty tokens.
   */
  @Test
  public void emptyPathsTest() throws Exception {
    TestGCE gce = new TestGCE();

    gce.addCandidate("hdfs://foo:6000/accumulo/tables/4//t0//F000.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4//t0//F001.rf");
    var candidate = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5//t0//F005.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo//tables//6/t0/F006.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4//t0//F000.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4//t0//F001.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo//tables//6/t0/F006.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gca.collect(gce);

    assertFileDeleted(gce, candidate);
  }

  @Test
  public void testRelative() throws Exception {
    TestGCE gce = new TestGCE();

    var candOne = gce.addCandidate("/4/t0/F000.rf");
    var candTwo = gce.addCandidate("/4/t0/F002.rf");
    var candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");

    gce.addFileReference("4", null, "/t0/F000.rf");
    gce.addFileReference("4", null, "/t0/F001.rf");
    gce.addFileReference("4", null, "/t0/F002.rf");
    gce.addFileReference("5", null, "../4/t0/F000.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // All candidates currently have references
    gca.collect(gce);
    assertFileDeleted(gce);

    List<String[]> refsToRemove = new ArrayList<>();
    refsToRemove.add(new String[] {"4", "/t0/F000.rf"});
    refsToRemove.add(new String[] {"5", "../4/t0/F000.rf"});
    refsToRemove.add(new String[] {"6", "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf"});

    Collections.shuffle(refsToRemove);

    for (int i = 0; i < 2; i++) {
      gce.removeFileReference(refsToRemove.get(i)[0], null, refsToRemove.get(i)[1]);
      gca.collect(gce);
      assertFileDeleted(gce);
    }

    gce.removeFileReference(refsToRemove.get(2)[0], null, refsToRemove.get(2)[1]);
    gca.collect(gce);
    assertFileDeleted(gce, candOne);

    gce.removeFileReference("4", null, "/t0/F001.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candThree);

    // add absolute candidate for file that already has a relative candidate
    var candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce);

    gce.removeFileReference("4", null, "/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candFour);

    gca.collect(gce);
    assertFileDeleted(gce, candTwo);
  }

  @Test
  public void testBlip() throws Exception {
    TestGCE gce = new TestGCE();

    gce.addCandidate("/4/b-0");
    gce.addCandidate("/4/b-0/F002.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/b-0/F001.rf");
    gce.addCandidate("/5/b-0");
    gce.addCandidate("/5/b-0/F002.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/b-0/F001.rf");

    gce.blips.add("/4/b-0");
    gce.blips.add("hdfs://foo.com:6000/accumulo/tables/5/b-0");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // Nothing should be removed because all candidates exist within a blip
    gca.collect(gce);
    assertFileDeleted(gce);

    // Remove the first blip
    gce.blips.remove("/4/b-0");

    // And we should lose all files in that blip and the blip directory itself -- relative and
    // absolute
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/4/b-0", 0L), new GcCandidate("/4/b-0/F002.rf", 1L),
        new GcCandidate("hdfs://foo.com:6000/accumulo/tables/4/b-0/F001.rf", 2L));

    gce.blips.remove("hdfs://foo.com:6000/accumulo/tables/5/b-0");

    // Same as above, we should lose relative and absolute for a relative or absolute blip
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/5/b-0", 3L), new GcCandidate("/5/b-0/F002.rf", 4L),
        new GcCandidate("hdfs://foo.com:6000/accumulo/tables/5/b-0/F001.rf", 5L));

    gca.collect(gce);
    assertFileDeleted(gce);
  }

  @Test
  public void testDirectories() throws Exception {
    TestGCE gce = new TestGCE();

    gce.addCandidate("/4/t-0");
    gce.addCandidate("/4/t-0/F002.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.addCandidate("/6/t-0");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/7/t-0/");
    gce.addCandidate("/8/t-0");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/9/t-0");
    gce.addCandidate("/a/t-0");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/b/t-0");
    gce.addCandidate("/c/t-0");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/d/t-0");

    gce.addDirReference("4", null, "t-0");
    gce.addDirReference("5", null, "t-0");
    gce.addDirReference("6", null, "t-0");
    gce.addDirReference("7", null, "t-0");

    gce.addFileReference("8", "m", "/t-0/F00.rf");
    gce.addFileReference("9", "m", "/t-0/F00.rf");

    gce.addFileReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-0/F00.rf");
    gce.addFileReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-0/F00.rf");

    gce.addFileReference("e", "m", "../c/t-0/F00.rf");
    gce.addFileReference("f", "m", "../d/t-0/F00.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // A directory reference does not preclude a candidate file beneath that directory from deletion
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/4/t-0/F002.rf", 1L));

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("hdfs://foo.com:6000/accumulo/tables/5/t-0", 2L));

    gce.removeDirReference("4", null);
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/4/t-0", 0L));

    gce.removeDirReference("6", null);
    gce.removeDirReference("7", null);
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/6/t-0", 3L),
        new GcCandidate("hdfs://foo:6000/accumulo/tables/7/t-0/", 4L));

    gce.removeFileReference("8", "m", "/t-0/F00.rf");
    gce.removeFileReference("9", "m", "/t-0/F00.rf");
    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-0/F00.rf");
    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-0/F00.rf");
    gce.removeFileReference("e", "m", "../c/t-0/F00.rf");
    gce.removeFileReference("f", "m", "../d/t-0/F00.rf");
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/8/t-0", 5L),
        new GcCandidate("hdfs://foo:6000/accumulo/tables/9/t-0", 6L), new GcCandidate("/a/t-0", 7L),
        new GcCandidate("hdfs://foo:6000/accumulo/tables/b/t-0", 8L), new GcCandidate("/c/t-0", 9L),
        new GcCandidate("hdfs://foo:6000/accumulo/tables/d/t-0", 10L));

    gca.collect(gce);
    assertFileDeleted(gce);
  }

  @Test
  public void testCustomDirectories() throws Exception {
    TestGCE gce = new TestGCE();
    Map<Integer,GcCandidate> candidates = new HashMap<>();

    candidates.put(1, gce.addCandidate("/4/t-0"));
    candidates.put(2, gce.addCandidate("/4/t-0/F002.rf"));
    candidates.put(3, gce.addCandidate("hdfs://foo.com:6000/user/foo/tables/5/t-0"));
    candidates.put(4, gce.addCandidate("/6/t-0"));
    candidates.put(5, gce.addCandidate("hdfs://foo:6000/user/foo/tables/7/t-0/"));
    candidates.put(6, gce.addCandidate("/8/t-0"));
    candidates.put(7, gce.addCandidate("hdfs://foo:6000/user/foo/tables/9/t-0"));
    candidates.put(8, gce.addCandidate("/a/t-0"));
    candidates.put(9, gce.addCandidate("hdfs://foo:6000/user/foo/tables/b/t-0"));
    candidates.put(10, gce.addCandidate("/c/t-0"));
    candidates.put(11, gce.addCandidate("hdfs://foo:6000/user/foo/tables/d/t-0"));

    gce.addDirReference("4", null, "t-0");
    gce.addDirReference("5", null, "t-0");
    gce.addDirReference("6", null, "t-0");
    gce.addDirReference("7", null, "t-0");

    gce.addFileReference("8", "m", "/t-0/F00.rf");
    gce.addFileReference("9", "m", "/t-0/F00.rf");

    gce.addFileReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.addFileReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-0/F00.rf");

    gce.addFileReference("e", "m", "../c/t-0/F00.rf");
    gce.addFileReference("f", "m", "../d/t-0/F00.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // A directory reference does not preclude a candidate file beneath that directory from deletion
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(2));

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    // but we need to add a file ref
    gce.addFileReference("8", "m", "/t-0/F00.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(3));

    gce.removeDirReference("4", null);
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(1));

    gce.removeDirReference("6", null);
    gce.removeDirReference("7", null);
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(4), candidates.get(5));

    gce.removeFileReference("8", "m", "/t-0/F00.rf");
    gce.removeFileReference("9", "m", "/t-0/F00.rf");
    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-0/F00.rf");
    gce.removeFileReference("e", "m", "../c/t-0/F00.rf");
    gce.removeFileReference("f", "m", "../d/t-0/F00.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(6), candidates.get(7), candidates.get(8),
        candidates.get(9), candidates.get(10), candidates.get(11));

    gca.collect(gce);
    assertFileDeleted(gce);
  }

  private void badRefTest(String ref) {
    TestGCE gce = new TestGCE();

    gce.addCandidate("/4/t-0/F002.rf");

    gce.addFileReference("4", "m", ref);

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    try {
      gca.collect(gce);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testBadFileRef1() {
    assertThrows(IllegalArgumentException.class, () -> badRefTest("/F00.rf"));
  }

  @Test
  public void testBadFileRef2() {
    assertThrows(IllegalArgumentException.class, () -> badRefTest("../F00.rf"));
  }

  @Test
  public void testBadFileRef3() {
    assertThrows(IllegalArgumentException.class,
        () -> badRefTest("hdfs://foo.com:6000/accumulo/F00.rf"));
  }

  @Test
  public void testBadFileRef4() {
    assertThrows(IllegalArgumentException.class,
        () -> badRefTest("hdfs://foo.com:6000/accumulo/tbls/5/F00.rf"));
  }

  @Test
  public void testBadFileRef5() {
    assertThrows(RuntimeException.class, () -> badRefTest("F00.rf"));
  }

  @Test
  public void testBadFileRef6() {
    assertThrows(IllegalArgumentException.class, () -> badRefTest("/accumulo/tbls/5/F00.rf"));
  }

  @Test
  public void testBadDeletes() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();
    gce.addCandidate("");
    gce.addCandidate("A");
    gce.addCandidate("/");
    gce.addCandidate("//");
    gce.addCandidate("///");
    gce.addCandidate("////");
    gce.addCandidate("/1/2/3/4");
    gce.addCandidate("/a");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tbls/5/F00.rf");
    gce.addCandidate("hdfs://foo.com:6000/");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/");
    gce.addCandidate("hdfs://foo.com:6000/user/foo/tables/a/t-0/t-1/F00.rf");

    gca.collect(gce);
    System.out.println(gce.fileDeletions);
    assertFileDeleted(gce);
  }

  @Test
  public void test() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.addCandidate("/1636/default_tablet");
    gce.addDirReference("1636", null, "default_tablet");
    gca.collect(gce);
    assertFileDeleted(gce);

    gce.candidates.clear();
    var tempCandidate = gce.addCandidate("/1636/default_tablet/someFile");
    gca.collect(gce);
    assertFileDeleted(gce, tempCandidate);

    gce.addFileReference("1636", null, "/default_tablet/someFile");
    gce.addCandidate("/1636/default_tablet/someFile");
    gca.collect(gce);
    assertFileDeleted(gce);

    // have an indirect file reference
    gce = new TestGCE();

    gce.addFileReference("1636", null, "../9/default_tablet/someFile");
    gce.addDirReference("1636", null, "default_tablet");
    gce.addCandidate("/9/default_tablet/someFile");
    gca.collect(gce);
    assertFileDeleted(gce);

    // have an indirect file reference and a directory candidate
    gce.candidates.clear();
    gce.addCandidate("/9/default_tablet");
    gca.collect(gce);
    assertFileDeleted(gce);

    gce.candidates.clear();
    gce.addCandidate("/9/default_tablet");
    gce.addCandidate("/9/default_tablet/someFile");
    long blipCount = gca.collect(gce);
    assertFileDeleted(gce);
    assertEquals(0, blipCount);

    gce = new TestGCE();

    gce.blips.add("/1636/b-0001");
    gce.addCandidate("/1636/b-0001/I0000");
    blipCount = gca.collect(gce);
    assertFileDeleted(gce);
    assertEquals(1, blipCount);

    gce = new TestGCE();

    gce.blips.add("/1029/b-0001");
    gce.blips.add("/1029/b-0002");
    gce.blips.add("/1029/b-0003");
    gce.blips.add("/1000/b-1001");
    gce.blips.add("/1000/b-1002");
    gce.addCandidate("/1029/b-0002/I0006");
    gce.addCandidate("/1000/b-1002/I0007");
    var candidate = gce.addCandidate("/1000/t-0003/I0008");
    blipCount = gca.collect(gce);
    assertFileDeleted(gce, candidate);
    assertEquals(5, blipCount);
  }

  @Test
  public void testDeleteTableDir() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.tableIds.add(TableId.of("4"));

    gce.addCandidate("/4/t-0");
    gce.addCandidate("/4/t-0/F002.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.addCandidate("/6/t-0");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/7/t-0/");

    gce.addDirReference("4", null, "t-0");
    gce.addDirReference("7", null, "t-0");

    gca.collect(gce);

    HashSet<TableId> tids = new HashSet<>();
    tids.add(TableId.of("5"));
    tids.add(TableId.of("6"));

    assertEquals(tids.size(), gce.tablesDirsToDelete.size());
    assertTrue(tids.containsAll(gce.tablesDirsToDelete));
    assertNoCandidatesRemoved(gce);
  }

  @Test
  public void finishedReplicationRecordsDontPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    Status status = Status.newBuilder().setClosed(true).setEnd(100).setBegin(100).build();
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // No refs to A000002.rf, and a closed, finished repl for A000001.rf should not preclude
    // it from being deleted
    assertEquals(2, gce.fileDeletions.size());
  }

  @Test
  public void openReplicationRecordsPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    var candidate = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    // We replicated all of the data, but we might still write more data to the file
    Status status = Status.newBuilder().setClosed(false).setEnd(1000).setBegin(100).build();
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // We need to replicate that one file still, should not delete it.
    assertEquals(1, gce.fileDeletions.size());
    assertEquals(candidate, gce.fileDeletions.get(0));
  }

  @Test
  public void newReplicationRecordsPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    var candidate = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    // We replicated all of the data, but we might still write more data to the file
    @SuppressWarnings("deprecation")
    Status status =
        org.apache.accumulo.server.replication.StatusUtil.fileCreated(System.currentTimeMillis());
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // We need to replicate that one file still, should not delete it.
    assertEquals(1, gce.fileDeletions.size());
    assertEquals(candidate, gce.fileDeletions.get(0));
  }

  @Test
  public void bulkImportReplicationRecordsPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    assertEquals(0, gce.fileDeletions.size());

    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    // Some file of unknown length has no replication yet (representative of the bulk-import case)
    Status status = Status.newBuilder().setInfiniteEnd(true).setBegin(0).setClosed(true).build();
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // We need to replicate that one file still, should not delete it.
    assertEquals(1, gce.fileDeletions.size());
    assertEquals(new GcCandidate("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf", 1L),
        gce.fileDeletions.get(0));
  }

  @Test
  public void testMissingTableIds() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE(Ample.DataLevel.USER);

    gce.addCandidate("hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");

    gce.addFileReference("a", null, "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.addFileReference("c", null, "hdfs://foo.com:6000/user/foo/tables/c/t-0/F00.rf");

    // add 2 more table references that will not be seen by the scan
    gce.tableIds.addAll(makeUnmodifiableSet("b", "d"));

    String msg = assertThrows(RuntimeException.class, () -> gca.collect(gce)).getMessage();
    assertTrue((msg.contains("[b, d]") || msg.contains("[d, b]"))
        && msg.contains("Saw table IDs in ZK that were not in metadata table:"), msg);
  }

  @Test
  public void testDeletingInUseReferenceCandidates() throws Exception {
    TestGCE gce = new TestGCE();

    var candidate = gce.addCandidate("/4/t0/F000.rf");

    gce.addFileReference("4", null, "/t0/F000.rf");
    gce.addFileReference("4", null, "/t0/F001.rf");
    gce.addFileReference("4", null, "/t0/F002.rf");
    gce.addFileReference("4", null, "/t0/F003.rf");
    gce.addFileReference("5", null, "../4/t0/F000.rf");
    gce.addFileReference("9", null, "/t0/F003.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gce.deleteInUseRefs = false;
    // All candidates currently have references
    gca.collect(gce);
    assertFileDeleted(gce);
    assertNoCandidatesRemoved(gce);

    // Enable InUseRefs to be removed if the file ref is found.
    gce.deleteInUseRefs = true;
    gca.collect(gce);
    assertFileDeleted(gce);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candidate);

    var cand1 = gce.addCandidate("/9/t0/F003.rf");
    var cand2 = gce.addCandidate("../144/t0/F003.rf");
    gce.removeFileReference("9", null, "/t0/F003.rf");
    gce.removeFileReference("4", null, "/t0/F003.rf");

    gca.collect(gce);
    assertNoCandidatesRemoved(gce);
    // File references did not exist, so candidates are processed
    assertFileDeleted(gce, cand1, cand2);
  }

  @Test
  public void testDeletingRootInUseReferenceCandidates() throws Exception {
    TestGCE gce = new TestGCE(Ample.DataLevel.ROOT);

    GcCandidate[] toBeRemoved = new GcCandidate[4];

    toBeRemoved[0] = gce.addCandidate("/+r/t0/F000.rf");
    toBeRemoved[1] = gce.addCandidate("/+r/t0/F001.rf");
    toBeRemoved[2] = gce.addCandidate("/+r/t0/F002.rf");
    toBeRemoved[3] = gce.addCandidate("/+r/t0/F003.rf");

    gce.addFileReference("+r", null, "/t0/F000.rf");
    gce.addFileReference("+r", null, "/t0/F001.rf");
    gce.addFileReference("+r", null, "/t0/F002.rf");
    gce.addFileReference("+r", null, "/t0/F003.rf");
    gce.addFileReference("+r", null, "../4/t0/F000.rf");
    gce.addFileReference("+r", null, "/t0/F003.rf");
    gce.addFileReference("+r", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gce.deleteInUseRefs = false;
    // No InUse Candidates should be removed.
    gca.collect(gce);
    assertFileDeleted(gce);
    assertNoCandidatesRemoved(gce);

    gce.deleteInUseRefs = true;
    // Due to the gce Datalevel of ROOT, InUse candidate deletion is not supported regardless of
    // property setting.
    gca.collect(gce);
    assertFileDeleted(gce);
    assertNoCandidatesRemoved(gce);

    gce.removeFileReference("+r", null, "/t0/F000.rf");
    gce.removeFileReference("+r", null, "/t0/F001.rf");
    gce.removeFileReference("+r", null, "/t0/F002.rf");
    gce.removeFileReference("+r", null, "/t0/F003.rf");

    // With file references deleted, the GC should now process the candidates
    gca.collect(gce);
    assertFileDeleted(gce, toBeRemoved);
    assertNoCandidatesRemoved(gce);
  }

  @Test
  public void testInUseDirReferenceCandidates() throws Exception {
    TestGCE gce = new TestGCE();
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // Check expected starting state.
    assertEquals(0, gce.candidates.size());

    // Ensure that dir candidates still work
    var candOne = gce.addCandidate("6/t-0");
    var candTwo = gce.addCandidate("7/T-1");
    gce.addDirReference("6", null, "t-0");

    gca.collect(gce);
    assertFileDeleted(gce, candTwo);
    assertNoCandidatesRemoved(gce);
    assertEquals(1, gce.candidates.size());

    // Removing the dir reference causes the dir to be deleted.
    gce.removeDirReference("6", null);

    gca.collect(gce);
    assertFileDeleted(gce, candOne);
    assertNoCandidatesRemoved(gce);

    assertEquals(0, gce.candidates.size());

    // Now enable InUse deletions
    gce.deleteInUseRefs = true;

    // Add deletion candidate for a directory.
    var candidate = new GcCandidate("6/t-0/", 10L);
    gce.candidates.add(candidate);

    // Then create a InUse candidate for a file in that directory.
    gce.addFileReference("6", null, "/t-0/F003.rf");
    var removedCandidate = gce.addCandidate("6/t-0/F003.rf");

    gca.collect(gce);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, removedCandidate);
    assertFileDeleted(gce);
    // Check and make sure the InUse directory candidates are not removed.
    assertEquals(1, gce.candidates.size());
    assertTrue(gce.candidates.contains(candidate));
  }

  @Test
  public void testInUseScanReferenceCandidates() throws Exception {
    TestGCE gce = new TestGCE();

    // InUse Scan Refs should not be removed.
    var scanCandidate = gce.addCandidate("/4/t0/F010.rf");
    var candOne = gce.addCandidate("/4/t0/F000.rf");
    var candTwo = gce.addCandidate("/6/t0/F123.rf");
    gce.addScanReference("4", null, "/t0/F010.rf");
    gce.addFileReference("4", null, "/t0/F000.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gce.deleteInUseRefs = true;

    gca.collect(gce);
    assertFileDeleted(gce, candTwo);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candOne);
    assertEquals(Set.of(scanCandidate), gce.candidates);

    gce.removeScanReference("4", null, "/t0/F010.rf");
    gca.collect(gce);
    assertFileDeleted(gce, scanCandidate);
    assertNoCandidatesRemoved(gce);
    assertEquals(0, gce.candidates.size());
  }

  // below are tests for potential failure conditions of the GC process. Some of these cases were
  // observed on clusters. Some were hypothesis based on observations. The result was that
  // candidate entries were not removed when they should have been and therefore files were
  // removed from HDFS that were actually still in use

  private Set<TableId> makeUnmodifiableSet(String... args) {
    Set<TableId> t = new HashSet<>();
    for (String arg : args) {
      t.add(TableId.of(arg));
    }
    return Collections.unmodifiableSet(t);
  }

  /**
   * happy path, no tables added or removed during this portion and all the tables checked
   */
  @Test
  public void testNormalGCRun() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<TableId> tablesSeen = makeUnmodifiableSet("2", "1", "3");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "3", "2");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  /**
   * table was added during this portion and we don't see it, should be fine
   */
  @Test
  public void testTableAddedInMiddle() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<TableId> tablesSeen = makeUnmodifiableSet("2", "1", "3");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "3", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  /**
   * Test that a table was added during this portion and we see it. This means that the table was
   * added after the candidates were grabbed, so there should be nothing to remove
   */
  @Test
  public void testTableAddedInMiddleTwo() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<TableId> tablesSeen = makeUnmodifiableSet("2", "1", "3", "4");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "3", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  /**
   * Test that the table was deleted during this portion and we don't see it. This means any
   * candidates from the deleted table will stay on the candidate list and during the delete step
   * they will try to removed
   */
  @Test
  public void testTableDeletedInMiddle() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<TableId> tablesSeen = makeUnmodifiableSet("2", "1", "4");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  /**
   * table was deleted during this portion and we DO see it this mean candidates from the deleted
   * table may get removed from the candidate list which should be ok, as the delete table function
   * should be responsible for removing those
   */
  @Test
  public void testTableDeletedInMiddleTwo() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<TableId> tablesSeen = makeUnmodifiableSet("2", "1", "4", "3");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  /**
   * this test simulates missing an entire table when looking for what files are in use if you add
   * custom splits to the metadata at able boundaries, this can happen with a failed scan recall the
   * ~tab:~pr for this first entry of a new table is empty, so there is now way to check the prior
   * row. If you split a couple of tables in the metadata the table boundary , say table ids 2,3,4,
   * and then miss scanning table 3 but get 4, it is possible other consistency checks will miss
   * this
   */
  @Test
  public void testMissEntireTable() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<TableId> tablesSeen = makeUnmodifiableSet("1", "2", "4");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "2", "3", "4");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.startsWith("Saw table IDs in ZK that were not in metadata table:"));

  }

  /**
   * this test simulates getting nothing from ZK for table ids, which should not happen, but just in
   * case let's test
   */
  @Test
  public void testZKHadNoTables() {
    Set<TableId> tablesBefore = makeUnmodifiableSet();
    Set<TableId> tablesSeen = makeUnmodifiableSet("1", "2");
    Set<TableId> tablesAfter = makeUnmodifiableSet();

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.startsWith("Garbage collection will not proceed because"));
  }

  /**
   * simulates missing a table when checking references, and a table being added
   */
  @Test
  public void testMissingTableAndTableAdd() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<TableId> tablesSeen = makeUnmodifiableSet("1", "2", "4");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "2", "3", "4");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.startsWith("Saw table IDs in ZK that were not in metadata table:  [3]"));
  }

  /**
   * simulates missing a table when checking references, and a table being deleted
   */
  @Test
  public void testMissingTableAndTableDeleted() {
    Set<TableId> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<TableId> tablesSeen = makeUnmodifiableSet("1", "2", "4");
    Set<TableId> tablesAfter = makeUnmodifiableSet("1", "2", "3");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.startsWith("Saw table IDs in ZK that were not in metadata table:  [3]"));

  }
}
