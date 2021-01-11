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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
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

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.junit.Test;

public class GarbageCollectionTest {
  static class TestGCE implements GarbageCollectionEnvironment {
    TreeSet<String> candidates = new TreeSet<>();
    ArrayList<String> blips = new ArrayList<>();
    Map<String,Reference> references = new TreeMap<>();
    HashSet<TableId> tableIds = new HashSet<>();

    ArrayList<String> deletes = new ArrayList<>();
    ArrayList<TableId> tablesDirsToDelete = new ArrayList<>();
    TreeMap<String,Status> filesToReplicate = new TreeMap<>();

    @Override
    public boolean getCandidates(String continuePoint, List<String> ret) {
      Iterator<String> iter = candidates.tailSet(continuePoint, false).iterator();
      while (iter.hasNext() && ret.size() < 3) {
        ret.add(iter.next());
      }

      return ret.size() == 3;
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
    public Set<TableId> getTableIDs() {
      return tableIds;
    }

    @Override
    public void delete(SortedMap<String,String> candidateMap) {
      deletes.addAll(candidateMap.values());
      this.candidates.removeAll(candidateMap.values());
    }

    @Override
    public void deleteTableDirIfEmpty(TableId tableID) {
      tablesDirsToDelete.add(tableID);
    }

    public void addFileReference(String tableId, String endRow, String file) {
      references.put(tableId + ":" + endRow + ":" + file,
          new Reference(TableId.of(tableId), file, false));
    }

    public void removeFileReference(String tableId, String endRow, String file) {
      references.remove(tableId + ":" + endRow + ":" + file);
    }

    public void addDirReference(String tableId, String endRow, String dir) {
      references.put(tableId + ":" + endRow, new Reference(TableId.of(tableId), dir, true));
    }

    public void removeDirReference(String tableId, String endRow) {
      references.remove(tableId + ":" + endRow);
    }

    @Override
    public void incrementCandidatesStat(long i) {}

    @Override
    public void incrementInUseStat(long i) {}

    @Override
    public Iterator<Entry<String,Status>> getReplicationNeededIterator() {
      return filesToReplicate.entrySet().iterator();
    }
  }

  private void assertRemoved(TestGCE gce, String... refs) {
    for (String ref : refs) {
      assertTrue(gce.deletes.remove(ref));
    }

    assertEquals(0, gce.deletes.size());
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

  @Test(expected = IllegalArgumentException.class)
  public void testBadFileRef1() {
    badRefTest("/F00.rf");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFileRef2() {
    badRefTest("../F00.rf");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFileRef3() {
    badRefTest("hdfs://foo.com:6000/accumulo/F00.rf");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFileRef4() {
    badRefTest("hdfs://foo.com:6000/accumulo/tbls/5/F00.rf");
  }

  @Test(expected = RuntimeException.class)
  public void testBadFileRef5() {
    badRefTest("F00.rf");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadFileRef6() {
    badRefTest("/accumulo/tbls/5/F00.rf");
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
    gca.collect(gce);
    assertRemoved(gce);

    gce = new TestGCE();
    gce.blips.add("/1636/b-0001");
    gce.candidates.add("/1636/b-0001/I0000");
    gca.collect(gce);
    assertRemoved(gce);

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

    gce.addDirReference("7", null, "t-0");

    gca.collect(gce);

    HashSet<TableId> tids = new HashSet<>();
    tids.add(TableId.of("5"));
    tids.add(TableId.of("6"));

    assertEquals(tids.size(), gce.tablesDirsToDelete.size());
    assertTrue(tids.containsAll(gce.tablesDirsToDelete));

  }

  @Test
  public void finishedReplicationRecordsDontPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    Status status = Status.newBuilder().setClosed(true).setEnd(100).setBegin(100).build();
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // No refs to A000002.rf, and a closed, finished repl for A000001.rf should not preclude
    // it from being deleted
    assertEquals(2, gce.deletes.size());
  }

  @Test
  public void openReplicationRecordsPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    // We replicated all of the data, but we might still write more data to the file
    Status status = Status.newBuilder().setClosed(false).setEnd(1000).setBegin(100).build();
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // We need to replicate that one file still, should not delete it.
    assertEquals(1, gce.deletes.size());
    assertEquals("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf", gce.deletes.get(0));
  }

  @Test
  public void newReplicationRecordsPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    // We replicated all of the data, but we might still write more data to the file
    Status status = StatusUtil.fileCreated(System.currentTimeMillis());
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // We need to replicate that one file still, should not delete it.
    assertEquals(1, gce.deletes.size());
    assertEquals("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf", gce.deletes.get(0));
  }

  @Test
  public void bulkImportReplicationRecordsPreventDeletion() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf");

    // Some file of unknown length has no replication yet (representative of the bulk-import case)
    Status status = Status.newBuilder().setInfiniteEnd(true).setBegin(0).setClosed(true).build();
    gce.filesToReplicate.put("hdfs://foo.com:6000/accumulo/tables/1/t-00001/A000001.rf", status);

    gca.collect(gce);

    // We need to replicate that one file still, should not delete it.
    assertEquals(1, gce.deletes.size());
    assertEquals("hdfs://foo.com:6000/accumulo/tables/2/t-00002/A000002.rf", gce.deletes.get(0));
  }
}
