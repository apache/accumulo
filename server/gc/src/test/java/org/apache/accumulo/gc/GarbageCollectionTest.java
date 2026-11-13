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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.GcCandidateType;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class GarbageCollectionTest {

  static class TestGCE implements GarbageCollectionEnvironment {
    TreeSet<GcCandidate> candidates = new TreeSet<>();
    TreeMap<GcCandidate,GcCandidateType> deletedCandidates = new TreeMap<>();
    ArrayList<String> blips = new ArrayList<>();
    Map<String,Reference> references = new TreeMap<>();
    HashSet<TableId> tableIds = new HashSet<>();

    ArrayList<GcCandidate> fileDeletions = new ArrayList<>();
    private long timestamp = 0L;

    private final Ample.DataLevel level;

    TestGCE(Ample.DataLevel level) {
      this.level = level;
    }

    TestGCE() {
      this.level = Ample.DataLevel.USER;
    }

    public GcCandidate addCandidate(String s) {
      var candidate = new GcCandidate(s, timestamp,
          s.endsWith(".rf") ? GcCandidate.Type.TABLET_FILE : GcCandidate.Type.TABLET_DIR);
      this.candidates.add(candidate);
      this.timestamp = timestamp + 1;
      return candidate;
    }

    public void addCandidate(GcCandidate candidate) {
      this.candidates.add(candidate);
    }

    @Override
    public Iterator<GcCandidate> getCandidates() throws TableNotFoundException {
      return List.copyOf(candidates).iterator();
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
      tableIds.forEach(t -> results.put(t, TableState.ONLINE));
      return results;
    }

    @Override
    public void deleteConfirmedCandidates(Collection<GcCandidate> candidateMap) {
      fileDeletions.addAll(candidateMap);
      this.candidates.removeAll(candidateMap);
    }

    public void addFileReference(String tableId, String endRow, String file) {
      TableId tid = TableId.of(tableId);
      references.put(tableId + ":" + endRow + ":" + file,
          ReferenceFile.forFile(tid, new Path(file)));
      tableIds.add(tid);
    }

    public void removeFileReference(String tableId, String endRow, String file) {
      references.remove(tableId + ":" + endRow + ":" + file);
      removeLastTableIdRef(TableId.of(tableId));
    }

    public void addDirReference(String tableId, String endRow, String dir) {
      TableId tid = TableId.of(tableId);
      references.put(tableId + ":" + endRow, ReferenceFile.forDirectory(tid, dir));
      tableIds.add(tid);
    }

    public void removeDirReference(String tableId, String endRow) {
      references.remove(tableId + ":" + endRow);
      removeLastTableIdRef(TableId.of(tableId));
    }

    public void addScanReference(String tableId, String endRow, String scan) {
      TableId tid = TableId.of(tableId);
      references.put(tableId + ":" + endRow + ":scan:" + scan,
          ReferenceFile.forScan(tid, new Path(scan)));
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
    public Set<TableId> getCandidateTableIDs() {
      if (level == Ample.DataLevel.ROOT) {
        return Set.of(SystemTables.ROOT.tableId());
      } else if (level == Ample.DataLevel.METADATA) {
        return Collections.singleton(SystemTables.METADATA.tableId());
      } else if (level == Ample.DataLevel.USER) {
        Set<TableId> tableIds = new HashSet<>();
        getTableIDs().forEach((k, v) -> {
          if (v == TableState.ONLINE || v == TableState.OFFLINE) {
            // Don't return tables that are NEW, DELETING, or in an
            // UNKNOWN state.
            tableIds.add(k);
          }
        });
        tableIds.remove(SystemTables.METADATA.tableId());
        tableIds.remove(SystemTables.ROOT.tableId());
        return tableIds;
      } else {
        throw new IllegalArgumentException("unknown level " + level);
      }
    }
  }

  private void assertFileDeleted(TestGCE gce, GcCandidate... candidates) {
    for (GcCandidate candidate : candidates) {
      assertTrue(gce.fileDeletions.remove(candidate),
          "Did not remove candidate : " + candidate.toString());
    }

    assertEquals(0, gce.fileDeletions.size(), "Deletes not empty: " + gce.fileDeletions);
  }

  private void assertNoCandidatesRemoved(TestGCE gce) {
    assertEquals(0, gce.deletedCandidates.size(),
        "Deleted Candidates not empty: " + gce.deletedCandidates.entrySet());
  }

  private void assertCandidateRemoved(TestGCE gce, GcCandidateType gcCandidateType,
      GcCandidate... gcCandidates) {
    for (GcCandidate gcCandidate : gcCandidates) {
      assertEquals(gcCandidateType, gce.deletedCandidates.remove(gcCandidate));
    }
    assertEquals(0, gce.deletedCandidates.size(),
        "Deleted Candidates not empty: " + gce.deletedCandidates.entrySet());
  }

  // This test was created to help track down a ConcurrentModificationException error that was
  // occurring with the unit tests once the GC was refactored to use a single iterator for the
  // collect process. This was a minimal test case that would cause the exception to occur.
  @Test
  public void minimalDelete() throws Exception {
    TestGCE gce = new TestGCE();

    var candOne = gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    var candTwo = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    var candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");
    var candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/6/t0/F006.rf");

    gce.addFileReference("4", null, candOne.getPath());
    gce.addFileReference("4", null, candTwo.getPath());
    gce.addFileReference("6", null, candFour.getPath());
    gce.addDirReference("4", null, "t0");
    gce.addDirReference("6", null, "t1");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gca.collect(gce);

    assertFileDeleted(gce, candThree);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candOne, candTwo, candFour);
  }

  @Test
  public void testBasic() throws Exception {
    TestGCE gce = new TestGCE();

    var candOne = gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    var candTwo = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    var candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    gce.addDirReference("4", null, "t0");
    gce.addDirReference("5", null, "t0");
    gce.addFileReference("4", null, candOne.getPath());
    gce.addFileReference("4", null, candTwo.getPath());
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0//F002.rf");
    gce.addFileReference("5", null, candThree.getPath());

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // Remove the reference to a file in the candidates should cause it to be removed
    gce.removeFileReference("4", null, candOne.getPath());
    gca.collect(gce);
    assertFileDeleted(gce, candOne);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candTwo, candThree);

    // Removing a reference to a file that wasn't in the candidates should do nothing
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce);

    // Adding more candidates which do not have references that should be removed
    var candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F003.rf");
    var candFive = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F004.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candFour, candFive);
    assertNoCandidatesRemoved(gce);

    // Removing a reference to a file that wasn't in the candidates should do nothing
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce);
    assertNoCandidatesRemoved(gce);
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

    BiFunction<String,String,String> genPath =
        (id, f) -> String.format("hdfs://foo:6000/accumulo/tables/%s/t%s/F00%s.rf", id, id, f);

    var candOne = gce.addCandidate(genPath.apply("4", "0"));
    var candTwo = gce.addCandidate(genPath.apply("4", "1"));
    var candThree = gce.addCandidate(genPath.apply("5", "5"));

    int counter = 0;
    // items to be removed from candidates
    List<String> paths = new ArrayList<>();
    for (int i = 10; i < 30; i++) {
      paths.add(genPath.apply("" + i, "" + i));
    }

    for (String path : paths) {
      toBeRemoved[counter] = gce.addCandidate(path);
      counter++;
    }

    gce.addFileReference("4", null, candOne.getPath());
    gce.addFileReference("4", null, candTwo.getPath());
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0//F002.rf");
    gce.addFileReference("5", null, candThree.getPath());
    gce.addDirReference("4", null, "t4");
    gce.addDirReference("5", null, "t5");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    gca.collect(gce);
    assertFileDeleted(gce, toBeRemoved);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candOne, candTwo, candThree);

    // Add the candidate back in for the file removal
    candOne = gce.addCandidate(genPath.apply("4", "0"));
    // Remove the reference to a file in the candidates should cause it to be removed
    gce.removeFileReference("4", null, candOne.getPath());
    gca.collect(gce);
    assertFileDeleted(gce, candOne);
    assertNoCandidatesRemoved(gce);

    // Removing a reference to a file that wasn't in the candidates should do nothing
    gce.removeFileReference("4", null, genPath.apply("4", "2"));
    gca.collect(gce);
    assertFileDeleted(gce);
    assertNoCandidatesRemoved(gce);

    // Remove the reference to a file in the candidates should cause it to be removed
    candTwo = gce.addCandidate(genPath.apply("4", "1"));
    gce.removeFileReference("4", null, candTwo.getPath());
    gca.collect(gce);
    assertFileDeleted(gce, candTwo);
    assertNoCandidatesRemoved(gce);

    // Adding more candidates which do no have references should be removed
    var candFour = gce.addCandidate(genPath.apply("4", "3"));
    var candFive = gce.addCandidate(genPath.apply("4", "4"));
    gca.collect(gce);
    assertFileDeleted(gce, candFour, candFive);
    assertNoCandidatesRemoved(gce);
  }

  /**
   * Tests valid file paths that have empty tokens.
   */
  @Test
  public void emptyPathsTest() throws Exception {
    TestGCE gce = new TestGCE();

    var candOne = gce.addCandidate("hdfs://foo:6000/accumulo/tables/4//t0//F000.rf");
    var candTwo = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4//t0//F001.rf");
    var candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5//t0//F005.rf");
    var candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo//tables//6/t0/F006.rf");

    gce.addFileReference("4", null, candOne.getPath());
    gce.addFileReference("4", null, candTwo.getPath());
    gce.addFileReference("6", null, candFour.getPath());
    gce.addDirReference("4", null, "t0");
    gce.addDirReference("6", null, "t1");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gca.collect(gce);

    assertFileDeleted(gce, candThree);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candOne, candTwo, candFour);
  }

  @Test
  public void testAbsoluteReferencesWithRelativeCandidates() throws Exception {
    TestGCE gce = new TestGCE();

    var candOne = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    var candTwo = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    var candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gce.addFileReference("5", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addDirReference("4", null, "t0");
    gce.addDirReference("5", null, "t1");
    gce.addDirReference("6", null, "t1");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    List<String[]> refsToRemove = new ArrayList<>();
    refsToRemove.add(new String[] {"4", "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf"});
    refsToRemove.add(new String[] {"5", "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf"});
    refsToRemove.add(new String[] {"6", "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf"});

    Collections.shuffle(refsToRemove);

    for (int i = 0; i < 2; i++) {
      gce.removeFileReference(refsToRemove.get(i)[0], null, refsToRemove.get(i)[1]);
      gca.collect(gce);
      assertFileDeleted(gce);
    }
    // InUse references have been removed
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candOne, candTwo, candThree);

    gce.removeFileReference(refsToRemove.get(2)[0], null, refsToRemove.get(2)[1]);
    var cand = gce.addCandidate(refsToRemove.get(2)[1]);
    gca.collect(gce);
    assertFileDeleted(gce, cand);

    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    candThree = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candThree);
    assertNoCandidatesRemoved(gce);

    // add an absolute candidate for file that already has a relative candidate
    var candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candFour);

    // Re-add the absolute candidate
    candFour = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candFour);
    assertNoCandidatesRemoved(gce);

    // Finally re-add the relative candidate to remove the last file
    candTwo = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candTwo);
    assertNoCandidatesRemoved(gce);
  }

  @Test
  public void testBlip() throws Exception {
    TestGCE gce = new TestGCE();

    gce.addCandidate(GcCandidate.forBulkDir("hdfs://foo.com:6000/accumulo/tables/4/B-0"));
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/B-0/F002.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/B-0/F001.rf");
    gce.addCandidate(GcCandidate.forBulkDir("hdfs://foo.com:6000/accumulo/tables/5/b-1"));
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/b-1/F002.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/b-1/F001.rf");

    gce.blips.add("/4/B-0");
    gce.blips.add("/5/b-1");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // Nothing should be removed because all candidates exist within a blip
    gca.collect(gce);
    assertFileDeleted(gce);

    // Remove the first blip
    gce.blips.remove("/4/B-0");

    // And we should lose all files in that blip and the blip directory itself -- relative and
    // absolute
    gca.collect(gce);
    assertFileDeleted(gce, GcCandidate.forBulkDir("hdfs://foo.com:6000/accumulo/tables/4/B-0"),
        new GcCandidate("hdfs://foo.com:6000/accumulo/tables/4/B-0/F002.rf", 0L,
            GcCandidate.Type.TABLET_FILE),
        new GcCandidate("hdfs://foo.com:6000/accumulo/tables/4/B-0/F001.rf", 1L,
            GcCandidate.Type.TABLET_FILE));

    gce.blips.remove("/5/b-1");

    // Same as above, we should lose relative and absolute for a relative or absolute blip
    gca.collect(gce);
    assertFileDeleted(gce, GcCandidate.forBulkDir("hdfs://foo.com:6000/accumulo/tables/5/b-1"),
        new GcCandidate("hdfs://foo.com:6000/accumulo/tables/5/b-1/F002.rf", 2L,
            GcCandidate.Type.TABLET_FILE),
        new GcCandidate("hdfs://foo.com:6000/accumulo/tables/5/b-1/F001.rf", 3L,
            GcCandidate.Type.TABLET_FILE));

    gca.collect(gce);
    assertFileDeleted(gce);
  }

  @Test
  public void testDirectories() throws Exception {
    TestGCE gce = new TestGCE();

    gce.addCandidate("/4/t-4");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t-4/F002.rf");
    gce.addCandidate("/5/t-5");
    gce.addCandidate("/6/t-6");
    gce.addCandidate("/7/t-7/");
    gce.addCandidate("/8/t-8");
    gce.addCandidate("/9/t-9");
    gce.addCandidate("/a/t-a");
    gce.addCandidate("/b/t-b");
    gce.addCandidate("/c/t-c");
    gce.addCandidate("/d/t-d");

    gce.addDirReference("4", null, "t-4");
    gce.addDirReference("5", null, "t-5");
    gce.addDirReference("6", null, "t-6");
    gce.addDirReference("7", null, "t-7");

    gce.addDirReference("8", null, "t-82");
    gce.addDirReference("9", null, "t-92");
    gce.addDirReference("a", null, "t-a2");
    gce.addDirReference("b", null, "t-b2");
    gce.addDirReference("e", null, "t-e2");
    gce.addDirReference("f", null, "t-f2");

    gce.addFileReference("8", "m", "hdfs://foo:6000/accumulo/tables/8/t-8/F00.rf");
    gce.addFileReference("9", "m", "hdfs://foo:6000/accumulo/tables/9/t-9/F00.rf");

    gce.addFileReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-a/F00.rf");
    gce.addFileReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-b/F00.rf");

    gce.addFileReference("e", "m", "hdfs://foo:6000/accumulo/tables/c/t-c/F00.rf");
    gce.addFileReference("f", "m", "hdfs://foo:6000/accumulo/tables/d/t-d/F00.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // A directory reference does not preclude a candidate file beneath that directory from deletion
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("hdfs://foo.com:6000/accumulo/tables/4/t-4/F002.rf", 1L,
        GcCandidate.Type.TABLET_FILE));

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/5/t-5", 2L, GcCandidate.Type.TABLET_DIR));

    gce.removeDirReference("4", null);
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/4/t-4", 0L, GcCandidate.Type.TABLET_DIR));

    gce.removeDirReference("6", null);
    gce.removeDirReference("7", null);
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/6/t-6", 3L, GcCandidate.Type.TABLET_DIR),
        new GcCandidate("/7/t-7/", 4L, GcCandidate.Type.TABLET_DIR));

    gce.removeFileReference("8", "m", "hdfs://foo:6000/accumulo/tables/8/t-8/F00.rf");
    gce.removeFileReference("9", "m", "hdfs://foo:6000/accumulo/tables/9/t-9/F00.rf");
    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-a/F00.rf");
    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-b/F00.rf");
    gce.removeFileReference("e", "m", "hdfs://foo:6000/accumulo/tables/c/t-c/F00.rf");
    gce.removeFileReference("f", "m", "hdfs://foo:6000/accumulo/tables/d/t-d/F00.rf");
    gca.collect(gce);
    assertFileDeleted(gce, new GcCandidate("/8/t-8", 5L, GcCandidate.Type.TABLET_DIR),
        new GcCandidate("/9/t-9", 6L, GcCandidate.Type.TABLET_DIR),
        new GcCandidate("/a/t-a", 7L, GcCandidate.Type.TABLET_DIR),
        new GcCandidate("/b/t-b", 8L, GcCandidate.Type.TABLET_DIR),
        new GcCandidate("/c/t-c", 9L, GcCandidate.Type.TABLET_DIR),
        new GcCandidate("/d/t-d", 10L, GcCandidate.Type.TABLET_DIR));

    gca.collect(gce);
    assertFileDeleted(gce);
  }

  @Test
  public void testCustomDirectories() throws Exception {
    TestGCE gce = new TestGCE();
    Map<Integer,GcCandidate> candidates = new HashMap<>();

    candidates.put(1, gce.addCandidate("/4/t-0"));
    candidates.put(2, gce.addCandidate("hdfs://foo:6000/user/foo/tables/4/t-0/F002.rf"));
    candidates.put(3, gce.addCandidate("/5/t-5"));
    candidates.put(4, gce.addCandidate("/6/t-6"));
    candidates.put(5, gce.addCandidate("/7/t-7/"));
    candidates.put(6, gce.addCandidate("/8/t-8"));
    candidates.put(7, gce.addCandidate("/9/t-9"));
    candidates.put(8, gce.addCandidate("/a/t-a"));
    candidates.put(9, gce.addCandidate("/b/t-b"));
    candidates.put(10, gce.addCandidate("/c/t-c"));
    candidates.put(11, gce.addCandidate("/d/t-d"));

    gce.addDirReference("4", null, "t-0");
    gce.addDirReference("5", null, "t-5");
    gce.addDirReference("6", null, "t-6");
    gce.addDirReference("7", null, "t-7");
    gce.addDirReference("8", null, "t-82");
    gce.addDirReference("9", null, "t-92");
    gce.addDirReference("a", null, "t-a2");
    gce.addDirReference("b", null, "t-b2");
    gce.addDirReference("e", null, "t-e2");
    gce.addDirReference("f", null, "t-f2");

    gce.addFileReference("8", "m", "hdfs://foo:6000/accumulo/tables/8/t-8/F00.rf");
    gce.addFileReference("9", "m", "hdfs://foo:6000/accumulo/tables/9/t-9/F00.rf");

    gce.addFileReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-a/F00.rf");
    gce.addFileReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-b/F00.rf");

    gce.addFileReference("e", "m", "hdfs://foo.com:6000/user/foo/tables/c/t-c/F00.rf");
    gce.addFileReference("f", "m", "hdfs://foo.com:6000/user/foo/tables/d/t-d/F00.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // A directory reference does not preclude a candidate file beneath that directory from deletion
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(2));

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    // but we need to add a file ref
    gce.addFileReference("8", "m", "hdfs://foo:6000/accumulo/tables/8/t-8/F00.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(3));

    gce.removeDirReference("4", null);
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(1));

    gce.removeDirReference("6", null);
    gce.removeDirReference("7", null);
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(4), candidates.get(5));

    gce.removeFileReference("8", "m", "hdfs://foo:6000/accumulo/tables/8/t-8/F00.rf");
    gce.removeFileReference("9", "m", "hdfs://foo:6000/accumulo/tables/9/t-9/F00.rf");
    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-a/F00.rf");
    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-b/F00.rf");
    gce.removeFileReference("e", "m", "hdfs://foo.com:6000/user/foo/tables/c/t-c/F00.rf");
    gce.removeFileReference("f", "m", "hdfs://foo.com:6000/user/foo/tables/d/t-d/F00.rf");
    gca.collect(gce);
    assertFileDeleted(gce, candidates.get(6), candidates.get(7), candidates.get(8),
        candidates.get(9), candidates.get(10), candidates.get(11));

    gca.collect(gce);
    assertFileDeleted(gce);
  }

  private void badRefTest(String ref) {
    TestGCE gce = new TestGCE();

    gce.addCandidate("hdfs://foo:6000/accumulo/tables/8/4/t-0/F002.rf");

    gce.addFileReference("4", "m", ref);
    gce.addDirReference("4", "m", "t-0");

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
    var e = assertThrows(IllegalArgumentException.class, () -> badRefTest("/F00.rf"));
    assertTrue(e.getMessage().contains("scheme"));
  }

  @Test
  public void testRelativeFileRefIsInvalid() {
    var e = assertThrows(IllegalArgumentException.class, () -> badRefTest("/t-0/F00.rf"));
    assertTrue(e.getMessage().contains("scheme"));
  }

  @Test
  public void testBadFileRef2() {
    var e = assertThrows(IllegalArgumentException.class, () -> badRefTest("../F00.rf"));
    assertTrue(e.getMessage().contains("scheme"));
  }

  @Test
  public void testBadFileRef5() {
    var e = assertThrows(RuntimeException.class, () -> badRefTest("F00.rf"));
    assertTrue(e.getMessage().contains("scheme"));
  }

  @Test
  public void testBadFileRef6() {
    var e =
        assertThrows(IllegalArgumentException.class, () -> badRefTest("/accumulo/tbls/5/F00.rf"));
    assertTrue(e.getMessage().contains("scheme"));
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
    gce.addCandidate("/a");
    gce.addCandidate("hdfs://foo.com:6000/");

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
    var tempCandidate =
        gce.addCandidate("hdfs://foo:6000/accumulo/tables/1636/default_tablet/someFile.rf");
    gca.collect(gce);
    assertFileDeleted(gce, tempCandidate);

    gce.addFileReference("1636", null,
        "hdfs://foo:6000/accumulo/tables/1636/default_tablet/someFile.rf");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/1636/default_tablet/someFile.rf");
    gca.collect(gce);
    assertFileDeleted(gce);

    // have an indirect file reference
    gce = new TestGCE();

    gce.addFileReference("1636", null,
        "hdfs://foo:6000/accumulo/tables/9/default_tablet/someFile.rf");
    gce.addDirReference("1636", null, "default_tablet");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/9/default_tablet/someFile.rf");
    gca.collect(gce);
    assertFileDeleted(gce);

    // have an indirect file reference and a directory candidate
    gce.candidates.clear();
    gce.addCandidate("/9/default_tablet");
    gca.collect(gce);
    assertFileDeleted(gce);

    gce.candidates.clear();
    gce.addCandidate("/9/default_tablet");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/9/default_tablet/someFile.rf");
    long blipCount = gca.collect(gce);
    assertFileDeleted(gce);
    assertEquals(0, blipCount);

    gce = new TestGCE();

    gce.blips.add("/1636/b-0001");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/1636/b-0001/I0000.rf");
    blipCount = gca.collect(gce);
    assertFileDeleted(gce);
    assertEquals(1, blipCount);

    gce = new TestGCE();

    gce.blips.add("/1029/b-0001");
    gce.blips.add("/1029/b-0002");
    gce.blips.add("/1029/b-0003");
    gce.blips.add("/1000/b-1001");
    gce.blips.add("/1000/b-1002");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/1029/b-0002/I0006.rf");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/1000/b-1002/I0007.rf");
    var candidate = gce.addCandidate("hdfs://foo:6000/accumulo/tables/1000/t-0003/I0008.rf");
    blipCount = gca.collect(gce);
    assertFileDeleted(gce, candidate);
    assertEquals(5, blipCount);
  }

  @Test
  public void testDeleteTableDir() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.tableIds.add(TableId.of("4"));
    gce.tableIds.add(TableId.of("8"));

    gce.addCandidate("/4/t-0");
    gce.addCandidate(GcCandidate.forTableDir(TableId.of("5")));
    gce.addCandidate(GcCandidate.forTableDir(TableId.of("6")));
    gce.addCandidate(GcCandidate.forTableDir(TableId.of("8")));
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t-0/F002.rf");
    gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/6/t-0");
    gce.addCandidate("hdfs://foo:6000/accumulo/tables/7/t-0/");

    gce.addDirReference("4", null, "t-0");
    gce.addDirReference("7", null, "t-1");
    gce.addDirReference("8", null, "t-2");
    gce.addFileReference("8", null, "hdfs://foo:6000/accumulo/tables/8/t-0/");

    gca.collect(gce);

    HashSet<TableId> tids = new HashSet<>();
    tids.add(TableId.of("5"));
    tids.add(TableId.of("6"));

    var tableDirsDeleted = gce.fileDeletions.stream()
        .filter(gcc -> gcc.getType() == GcCandidate.Type.TABLE_DIR)
        .map(gcc -> gcc.getPath().replace("/", "")).map(TableId::of).collect(Collectors.toSet());
    assertEquals(tids, tableDirsDeleted);
    assertNoCandidatesRemoved(gce);
  }

  @Test
  public void testMissingTableIds() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE(Ample.DataLevel.USER);

    gce.addCandidate("hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");

    gce.addFileReference("a", null, "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.addDirReference("a", null, "t-0");
    gce.addDirReference("c", null, "t-1");

    // add 2 more table references that will not be seen by the scan
    gce.tableIds.addAll(makeUnmodifiableSet("b", "d"));

    String msg = assertThrows(RuntimeException.class, () -> gca.collect(gce)).getMessage();
    assertTrue((msg.contains("[b, d]") || msg.contains("[d, b]"))
        && msg.contains("Saw table IDs in ZK that were not in metadata table:"), msg);
  }

  @Test
  public void testDeletingInUseReferenceCandidates() throws Exception {
    TestGCE gce = new TestGCE();

    var candidate = gce.addCandidate("hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");

    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F002.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F003.rf");
    gce.addFileReference("5", null, "hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("9", null, "hdfs://foo:6000/accumulo/tables/9/t0/F003.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addDirReference("4", null, "t0");
    gce.addDirReference("5", null, "t0");
    gce.addDirReference("6", null, "t0");
    gce.addDirReference("9", null, "t0");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    // All candidates currently have references
    gca.collect(gce);
    assertFileDeleted(gce);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candidate);

    var cand1 = gce.addCandidate("hdfs://foo:6000/accumulo/tables/9/t2/F003.rf");
    var cand2 = gce.addCandidate("hdfs://foo:6000/accumulo/tables/144/t3/F003.rf");
    gce.removeFileReference("9", null, "hdfs://foo:6000/accumulo/tables/9/t0/F003.rf");
    gce.removeFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F003.rf");

    gca.collect(gce);
    assertNoCandidatesRemoved(gce);
    // File references did not exist, so candidates are processed
    assertFileDeleted(gce, cand1, cand2);
  }

  @Test
  public void testDeletingRootInUseReferenceCandidates() throws Exception {
    TestGCE gce = new TestGCE(Ample.DataLevel.ROOT);

    GcCandidate[] toBeRemoved = new GcCandidate[4];

    toBeRemoved[0] = gce.addCandidate("hdfs://foo:6000/accumulo/tables/+r/t0/F000.rf");
    toBeRemoved[1] = gce.addCandidate("hdfs://foo:6000/accumulo/tables/+r/t0/F001.rf");
    toBeRemoved[2] = gce.addCandidate("hdfs://foo:6000/accumulo/tables/+r/t0/F002.rf");
    toBeRemoved[3] = gce.addCandidate("hdfs://foo:6000/accumulo/tables/+r/t0/F003.rf");

    gce.addFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F000.rf");
    gce.addFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F001.rf");
    gce.addFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F002.rf");
    gce.addFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F003.rf");
    gce.addFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/4/t1/F000.rf");
    gce.addFileReference("+r", null, "hdfs://foo.com:6000/accumulo/tables/4/t1/F000.rf");

    gce.addDirReference("+r", null, "t0");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gca.collect(gce);
    assertFileDeleted(gce);
    // Due to the gce Datalevel of ROOT, InUse candidate deletion is not supported
    assertNoCandidatesRemoved(gce);

    gce.removeFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F000.rf");
    gce.removeFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F001.rf");
    gce.removeFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F002.rf");
    gce.removeFileReference("+r", null, "hdfs://foo:6000/accumulo/tables/+r/t0/F003.rf");

    // With file references deleted, the GC should now process the candidates
    gca.collect(gce);
    assertFileDeleted(gce, toBeRemoved);
    assertNoCandidatesRemoved(gce);
  }

  @Test
  public void testInUseDirReferenceCandidates() throws Exception {
    TestGCE gce = new TestGCE();
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // Check the expected starting state.
    assertEquals(0, gce.candidates.size());

    // Ensure that dir candidates still work
    var candOne = gce.addCandidate("6/t-0");
    var candTwo = gce.addCandidate("7/T-1");
    gce.addDirReference("6", "m", "t-1");
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

    // Add deletion candidate for a directory.
    var candidate = new GcCandidate("6/t-0/", 10L, GcCandidate.Type.TABLET_DIR);
    gce.candidates.add(candidate);

    // Then create an InUse candidate for a file in that directory.
    gce.addFileReference("6", null, "hdfs://foo:6000/accumulo/tables/6/t-0/F003.rf");
    var removedCandidate = gce.addCandidate("hdfs://foo:6000/accumulo/tables/6/t-0/F003.rf");

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
    var scanCandidate = gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t0/F010.rf");
    var candOne = gce.addCandidate("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    var candTwo = gce.addCandidate("hdfs://foo:6000/accumulo/tables/6/t0/F123.rf");
    gce.addDirReference("4", null, "t0");
    gce.addScanReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F010.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    gca.collect(gce);
    assertFileDeleted(gce, candTwo);
    assertCandidateRemoved(gce, GcCandidateType.INUSE, candOne);
    assertEquals(Set.of(scanCandidate), gce.candidates);

    gce.removeScanReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F010.rf");
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
