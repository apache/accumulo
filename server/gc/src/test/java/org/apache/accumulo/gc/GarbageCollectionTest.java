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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.gc.Reference;
import org.apache.accumulo.core.gc.ReferenceDirectory;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.junit.jupiter.api.Test;

public class GarbageCollectionTest {

  static class TestGCE implements GarbageCollectionEnvironment {
    TreeSet<String> candidates = new TreeSet<>();
    ArrayList<String> blips = new ArrayList<>();
    Map<String,Reference> references = new TreeMap<>();
    HashSet<TableId> tableIds = new HashSet<>();

    ArrayList<String> deletes = new ArrayList<>();
    ArrayList<TableId> tablesDirsToDelete = new ArrayList<>();

    private final Ample.DataLevel level;

    TestGCE(Ample.DataLevel level) {
      this.level = level;
    }

    TestGCE() {
      this.level = Ample.DataLevel.USER;
    }

    @Override
    public Iterator<String> getCandidates() throws TableNotFoundException {
      return List.copyOf(candidates).iterator();
    }

    @Override
    public List<String> readCandidatesThatFitInMemory(Iterator<String> candidatesIter) {
      List<String> candidatesBatch = new ArrayList<>();
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
    public Map<TableId,TableState> getTableIDs() {
      HashMap<TableId,TableState> results = new HashMap<>();
      tableIds.forEach(t -> results.put(t, TableState.ONLINE));
      return results;
    }

    @Override
    public void deleteConfirmedCandidates(SortedMap<String,String> candidateMap) {
      deletes.addAll(candidateMap.values());
      this.candidates.removeAll(candidateMap.values());
    }

    @Override
    public void deleteTableDirIfEmpty(TableId tableID) {
      tablesDirsToDelete.add(tableID);
    }

    public void addFileReference(String tableId, String endRow, String file) {
      TableId tid = TableId.of(tableId);
      references.put(tableId + ":" + endRow + ":" + file, new ReferenceFile(tid, file));
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

  private void assertRemoved(TestGCE gce, String... refs) {
    for (String ref : refs) {
      assertTrue(gce.deletes.remove(ref));
    }

    assertEquals(0, gce.deletes.size(), "Deletes not empty: " + gce.deletes);
  }

  // This test was created to help track down a ConcurrentModificationException error that was
  // occurring with the unit tests once the GC was refactored to use a single iterator for the
  // collect process. This was a minimal test case that would cause the exception to occur.
  @Test
  public void minimalDelete() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/6/t0/F006.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/6/t0/F006.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gca.collect(gce);

    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");
  }

  @Test
  public void testBasic() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0//F002.rf");
    gce.addFileReference("5", null, "hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    gca.collect(gce);
    assertRemoved(gce);

    // Remove the reference to this flush file, run the GC which should not trim it from the
    // candidates, and assert that it's gone
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");

    // Removing a reference to a file that wasn't in the candidates should do nothing
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertRemoved(gce);

    // Remove the reference to a file in the candidates should cause it to be removed
    gce.removeFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");

    // Adding more candidates which do no have references should be removed
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F003.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F004.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/4/t0/F003.rf",
        "hdfs://foo.com:6000/accumulo/tables/4/t0/F004.rf");

  }

  /*
   * Additional test with more candidates. Also, not a multiple of 3 as the test above. Since the
   * unit tests always return 3 candidates in a batch, some edge cases could be missed if that was
   * always the case.
   */
  @Test
  public void testBasic2() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/5/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/6/t1/F005.rf");

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/6/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/6/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/7/t0/F005.rf");

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/7/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/7/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/8/t0/F005.rf");

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/8/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/8/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/9/t0/F005.rf");

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/9/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/9/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/10/t0/F005.rf");

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/10/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/10/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/11/t0/F005.rf");

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/11/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/11/t0/F001.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0//F002.rf");
    gce.addFileReference("5", null, "hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    gca.collect(gce);
    // items to be removed from candidates
    String[] toBeRemoved = {"hdfs://foo.com:6000/accumulo/tables/5/t0/F001.rf",
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
    assertRemoved(gce, toBeRemoved);

    // Remove the reference to this flush file, run the GC which should not trim it from the
    // candidates, and assert that it's gone
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");

    // Removing a reference to a file that wasn't in the candidates should do nothing
    gce.removeFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertRemoved(gce);

    // Remove the reference to a file in the candidates should cause it to be removed
    gce.removeFileReference("4", null, "hdfs://foo:6000/accumulo/tables/4/t0/F001.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");

    // Adding more candidates which do no have references should be removed
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F003.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F004.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/4/t0/F003.rf",
        "hdfs://foo.com:6000/accumulo/tables/4/t0/F004.rf");
  }

  /**
   * Tests valid file paths that have empty tokens.
   */
  @Test
  public void emptyPathsTest() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/4//t0//F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4//t0//F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5//t0//F005.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo//tables//6/t0/F006.rf");

    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4//t0//F000.rf");
    gce.addFileReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4//t0//F001.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo//tables//6/t0/F006.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    gca.collect(gce);

    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/5//t0//F005.rf");
  }

  @Test
  public void testRelative() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("/4/t0/F000.rf");
    gce.candidates.add("/4/t0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");

    gce.addFileReference("4", null, "/t0/F000.rf");
    gce.addFileReference("4", null, "/t0/F001.rf");
    gce.addFileReference("4", null, "/t0/F002.rf");
    gce.addFileReference("5", null, "../4/t0/F000.rf");
    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // All candidates currently have references
    gca.collect(gce);
    assertRemoved(gce);

    List<String[]> refsToRemove = new ArrayList<>();
    refsToRemove.add(new String[] {"4", "/t0/F000.rf"});
    refsToRemove.add(new String[] {"5", "../4/t0/F000.rf"});
    refsToRemove.add(new String[] {"6", "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf"});

    Collections.shuffle(refsToRemove);

    for (int i = 0; i < 2; i++) {
      gce.removeFileReference(refsToRemove.get(i)[0], null, refsToRemove.get(i)[1]);
      gca.collect(gce);
      assertRemoved(gce);
    }

    gce.removeFileReference(refsToRemove.get(2)[0], null, refsToRemove.get(2)[1]);
    gca.collect(gce);
    assertRemoved(gce, "/4/t0/F000.rf");

    gce.removeFileReference("4", null, "/t0/F001.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");

    // add absolute candidate for file that already has a relative candidate
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");
    gca.collect(gce);
    assertRemoved(gce);

    gce.removeFileReference("4", null, "/t0/F002.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/4/t0/F002.rf");

    gca.collect(gce);
    assertRemoved(gce, "/4/t0/F002.rf");
  }

  @Test
  public void testBlip() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("/4/b-0");
    gce.candidates.add("/4/b-0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/b-0/F001.rf");
    gce.candidates.add("/5/b-0");
    gce.candidates.add("/5/b-0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/b-0/F001.rf");

    gce.blips.add("/4/b-0");
    gce.blips.add("hdfs://foo.com:6000/accumulo/tables/5/b-0");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // Nothing should be removed because all candidates exist within a blip
    gca.collect(gce);
    assertRemoved(gce);

    // Remove the first blip
    gce.blips.remove("/4/b-0");

    // And we should lose all files in that blip and the blip directory itself -- relative and
    // absolute
    gca.collect(gce);
    assertRemoved(gce, "/4/b-0", "/4/b-0/F002.rf",
        "hdfs://foo.com:6000/accumulo/tables/4/b-0/F001.rf");

    gce.blips.remove("hdfs://foo.com:6000/accumulo/tables/5/b-0");

    // Same as above, we should lose relative and absolute for a relative or absolute blip
    gca.collect(gce);
    assertRemoved(gce, "/5/b-0", "/5/b-0/F002.rf",
        "hdfs://foo.com:6000/accumulo/tables/5/b-0/F001.rf");

    gca.collect(gce);
    assertRemoved(gce);
  }

  @Test
  public void testDirectories() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("/4/t-0");
    gce.candidates.add("/4/t-0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.candidates.add("/6/t-0");
    gce.candidates.add("hdfs://foo:6000/accumulo/tables/7/t-0/");
    gce.candidates.add("/8/t-0");
    gce.candidates.add("hdfs://foo:6000/accumulo/tables/9/t-0");
    gce.candidates.add("/a/t-0");
    gce.candidates.add("hdfs://foo:6000/accumulo/tables/b/t-0");
    gce.candidates.add("/c/t-0");
    gce.candidates.add("hdfs://foo:6000/accumulo/tables/d/t-0");

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
    assertRemoved(gce, "/4/t-0/F002.rf");

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/5/t-0");

    gce.removeDirReference("4", null);
    gca.collect(gce);
    assertRemoved(gce, "/4/t-0");

    gce.removeDirReference("6", null);
    gce.removeDirReference("7", null);
    gca.collect(gce);
    assertRemoved(gce, "/6/t-0", "hdfs://foo:6000/accumulo/tables/7/t-0/");

    gce.removeFileReference("8", "m", "/t-0/F00.rf");
    gce.removeFileReference("9", "m", "/t-0/F00.rf");
    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-0/F00.rf");
    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-0/F00.rf");
    gce.removeFileReference("e", "m", "../c/t-0/F00.rf");
    gce.removeFileReference("f", "m", "../d/t-0/F00.rf");
    gca.collect(gce);
    assertRemoved(gce, "/8/t-0", "hdfs://foo:6000/accumulo/tables/9/t-0", "/a/t-0",
        "hdfs://foo:6000/accumulo/tables/b/t-0", "/c/t-0", "hdfs://foo:6000/accumulo/tables/d/t-0");

    gca.collect(gce);
    assertRemoved(gce);
  }

  @Test
  public void testCustomDirectories() throws Exception {
    TestGCE gce = new TestGCE();

    gce.candidates.add("/4/t-0");
    gce.candidates.add("/4/t-0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/user/foo/tables/5/t-0");
    gce.candidates.add("/6/t-0");
    gce.candidates.add("hdfs://foo:6000/user/foo/tables/7/t-0/");
    gce.candidates.add("/8/t-0");
    gce.candidates.add("hdfs://foo:6000/user/foo/tables/9/t-0");
    gce.candidates.add("/a/t-0");
    gce.candidates.add("hdfs://foo:6000/user/foo/tables/b/t-0");
    gce.candidates.add("/c/t-0");
    gce.candidates.add("hdfs://foo:6000/user/foo/tables/d/t-0");

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
    assertRemoved(gce, "/4/t-0/F002.rf");

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    // but we need to add a file ref
    gce.addFileReference("8", "m", "/t-0/F00.rf");
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/user/foo/tables/5/t-0");

    gce.removeDirReference("4", null);
    gca.collect(gce);
    assertRemoved(gce, "/4/t-0");

    gce.removeDirReference("6", null);
    gce.removeDirReference("7", null);
    gca.collect(gce);
    assertRemoved(gce, "/6/t-0", "hdfs://foo:6000/user/foo/tables/7/t-0/");

    gce.removeFileReference("8", "m", "/t-0/F00.rf");
    gce.removeFileReference("9", "m", "/t-0/F00.rf");
    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-0/F00.rf");
    gce.removeFileReference("e", "m", "../c/t-0/F00.rf");
    gce.removeFileReference("f", "m", "../d/t-0/F00.rf");
    gca.collect(gce);
    assertRemoved(gce, "/8/t-0", "hdfs://foo:6000/user/foo/tables/9/t-0", "/a/t-0",
        "hdfs://foo:6000/user/foo/tables/b/t-0", "/c/t-0", "hdfs://foo:6000/user/foo/tables/d/t-0");

    gca.collect(gce);
    assertRemoved(gce);
  }

  private void badRefTest(String ref) {
    TestGCE gce = new TestGCE();

    gce.candidates.add("/4/t-0/F002.rf");

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
    gce.candidates.add("");
    gce.candidates.add("A");
    gce.candidates.add("/");
    gce.candidates.add("//");
    gce.candidates.add("///");
    gce.candidates.add("////");
    gce.candidates.add("/1/2/3/4");
    gce.candidates.add("/a");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tbls/5/F00.rf");
    gce.candidates.add("hdfs://foo.com:6000/");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/");
    gce.candidates.add("hdfs://foo.com:6000/user/foo/tables/a/t-0/t-1/F00.rf");

    gca.collect(gce);
    System.out.println(gce.deletes);
    assertRemoved(gce);
  }

  @Test
  public void test() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.candidates.add("/1636/default_tablet");
    gce.addDirReference("1636", null, "default_tablet");
    gca.collect(gce);
    assertRemoved(gce);

    gce.candidates.clear();
    gce.candidates.add("/1636/default_tablet/someFile");
    gca.collect(gce);
    assertRemoved(gce, "/1636/default_tablet/someFile");

    gce.addFileReference("1636", null, "/default_tablet/someFile");
    gce.candidates.add("/1636/default_tablet/someFile");
    gca.collect(gce);
    assertRemoved(gce);

    // have an indirect file reference
    gce = new TestGCE();

    gce.addFileReference("1636", null, "../9/default_tablet/someFile");
    gce.addDirReference("1636", null, "default_tablet");
    gce.candidates.add("/9/default_tablet/someFile");
    gca.collect(gce);
    assertRemoved(gce);

    // have an indirect file reference and a directory candidate
    gce.candidates.clear();
    gce.candidates.add("/9/default_tablet");
    gca.collect(gce);
    assertRemoved(gce);

    gce.candidates.clear();
    gce.candidates.add("/9/default_tablet");
    gce.candidates.add("/9/default_tablet/someFile");
    long blipCount = gca.collect(gce);
    assertRemoved(gce);
    assertEquals(0, blipCount);

    gce = new TestGCE();

    gce.blips.add("/1636/b-0001");
    gce.candidates.add("/1636/b-0001/I0000");
    blipCount = gca.collect(gce);
    assertRemoved(gce);
    assertEquals(1, blipCount);

    gce = new TestGCE();

    gce.blips.add("/1029/b-0001");
    gce.blips.add("/1029/b-0002");
    gce.blips.add("/1029/b-0003");
    gce.blips.add("/1000/b-1001");
    gce.blips.add("/1000/b-1002");
    gce.candidates.add("/1029/b-0002/I0006");
    gce.candidates.add("/1000/b-1002/I0007");
    gce.candidates.add("/1000/t-0003/I0008");
    blipCount = gca.collect(gce);
    assertRemoved(gce, "/1000/t-0003/I0008");
    assertEquals(5, blipCount);
  }

  @Test
  public void testDeleteTableDir() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.tableIds.add(TableId.of("4"));

    gce.candidates.add("/4/t-0");
    gce.candidates.add("/4/t-0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.candidates.add("/6/t-0");
    gce.candidates.add("hdfs://foo:6000/accumulo/tables/7/t-0/");

    gce.addDirReference("4", null, "t-0");
    gce.addDirReference("7", null, "t-0");

    gca.collect(gce);

    HashSet<TableId> tids = new HashSet<>();
    tids.add(TableId.of("5"));
    tids.add(TableId.of("6"));

    assertEquals(tids.size(), gce.tablesDirsToDelete.size());
    assertTrue(tids.containsAll(gce.tablesDirsToDelete));

  }

  @Test
  public void testMissingTableIds() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE(Ample.DataLevel.USER);

    gce.candidates.add("hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");

    gce.addFileReference("a", null, "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.addFileReference("c", null, "hdfs://foo.com:6000/user/foo/tables/c/t-0/F00.rf");

    // add 2 more table references that will not be seen by the scan
    gce.tableIds.addAll(makeUnmodifiableSet("b", "d"));

    String msg = assertThrows(RuntimeException.class, () -> gca.collect(gce)).getMessage();
    assertTrue((msg.contains("[b, d]") || msg.contains("[d, b]"))
        && msg.contains("Saw table IDs in ZK that were not in metadata table:"), msg);
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
