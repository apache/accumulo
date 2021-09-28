/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.gc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 *
 */
public class GarbageCollectionTest {
  static class TestGCE implements GarbageCollectionEnvironment {
    TreeSet<String> candidates = new TreeSet<>();
    ArrayList<String> blips = new ArrayList<>();
    Map<Key,Value> references = new TreeMap<>();
    HashSet<String> tableIds = new HashSet<>();

    ArrayList<String> deletes = new ArrayList<>();
    ArrayList<String> tablesDirsToDelete = new ArrayList<>();
    TreeMap<String,Status> filesToReplicate = new TreeMap<>();

    private final String tableName;

    TestGCE(String tableName) {
      this.tableName = tableName;
    }

    TestGCE() {
      // assume metadata table if not passed in for tests
      this.tableName = MetadataTable.NAME;
    }

    @Override
    public boolean isRootTable() {
      return this.tableName == RootTable.NAME;
    }

    @Override
    public boolean isMetadataTable() {
      return this.tableName == MetadataTable.NAME;
    }

    @Override
    public boolean getCandidates(String continuePoint, List<String> ret) {
      Iterator<String> iter = candidates.tailSet(continuePoint, false).iterator();
      while (iter.hasNext() && ret.size() < 3) {
        ret.add(iter.next());
      }

      return ret.size() == 3;
    }

    @Override
    public Iterator<String> getBlipIterator() {
      return blips.iterator();
    }

    @Override
    public Iterator<Entry<Key,Value>> getReferenceIterator() {
      return references.entrySet().iterator();
    }

    @Override
    public Set<String> getTableIDs() {
      return tableIds;
    }

    @Override
    public void delete(SortedMap<String,String> candidateMap) {
      deletes.addAll(candidateMap.values());
      this.candidates.removeAll(candidateMap.values());
    }

    @Override
    public void deleteTableDirIfEmpty(String tableID) {
      tablesDirsToDelete.add(tableID);
    }

    public Key newFileReferenceKey(String tableId, String endRow, String file) {
      String row = new KeyExtent(tableId, endRow == null ? null : new Text(endRow), null)
          .getMetadataEntry().toString();
      String cf = MetadataSchema.TabletsSection.DataFileColumnFamily.NAME.toString();
      String cq = file;
      Key key = new Key(row, cf, cq);
      return key;
    }

    public Value addFileReference(String tableId, String endRow, String file) {
      Key key = newFileReferenceKey(tableId, endRow, file);
      Value val = references.put(key, new Value(new DataFileValue(0, 0).encode()));
      tableIds.add(tableId);
      return val;
    }

    public Value removeFileReference(String tableId, String endRow, String file) {
      Value retVal = references.remove(newFileReferenceKey(tableId, endRow, file));
      removeLastTableIdRef(tableId);
      return retVal;
    }

    Key newDirReferenceKey(String tableId, String endRow) {
      String row = new KeyExtent(tableId, endRow == null ? null : new Text(endRow), null)
          .getMetadataEntry().toString();
      String cf = MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN
          .getColumnFamily().toString();
      String cq = MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN
          .getColumnQualifier().toString();
      Key key = new Key(row, cf, cq);
      return key;
    }

    public Value addDirReference(String tableId, String endRow, String dir) {
      Key key = newDirReferenceKey(tableId, endRow);
      Value val = references.put(key, new Value(dir.getBytes()));
      tableIds.add(tableId);
      return val;
    }

    public Value removeDirReference(String tableId, String endRow) {
      Value retVal = references.remove(newDirReferenceKey(tableId, endRow));
      removeLastTableIdRef(tableId);
      return retVal;
    }

    /*
     * this is to be called from removeDirReference or removeFileReference.
     *
     * If you just removed the last reference to a table, we need to remove it from the tableIds in
     * zookeeper
     */
    private void removeLastTableIdRef(String tableId) {
      boolean inUse = references.keySet().stream()
          .map(k -> new String(KeyExtent.tableOfMetadataRow(k.getRow())))
          .anyMatch(tid -> tableId.equals(tid));
      if (!inUse) {
        assertTrue(tableIds.remove(tableId));
      }
    }

    @Override
    public void incrementCandidatesStat(long i) {}

    @Override
    public void incrementInUseStat(long i) {}

    @Override
    public Iterator<Entry<String,Status>> getReplicationNeededIterator()
        throws AccumuloException, AccumuloSecurityException {
      return filesToReplicate.entrySet().iterator();
    }

    @Override
    public Set<String> getCandidateTableIDs() {
      if (tableName.equals(RootTable.NAME)) {
        return Collections.singleton(MetadataTable.ID);
      } else {
        Set<String> tableIds = new HashSet<>(getTableIDs());
        tableIds.remove(MetadataTable.ID);
        tableIds.remove(RootTable.ID);
        return tableIds;
      }
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
    TestGCE gce = new TestGCE(MetadataTable.NAME);

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
    TestGCE gce = new TestGCE(MetadataTable.NAME);

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

    gce.addDirReference("4", null, "/t-0");
    gce.addDirReference("5", null, "/t-0");
    gce.addDirReference("6", null, "hdfs://foo.com:6000/accumulo/tables/6/t-0");
    gce.addDirReference("7", null, "hdfs://foo.com:6000/accumulo/tables/7/t-0");

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

    gce.addDirReference("4", null, "/t-0");
    gce.addDirReference("5", null, "/t-0");
    gce.addDirReference("6", null, "hdfs://foo.com:6000/user/foo/tables/6/t-0");
    gce.addDirReference("7", null, "hdfs://foo.com:6000/user/foo/tables/7/t-0");

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
    gce.addDirReference("1636", null, "/default_tablet");
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
    gce.addDirReference("1636", null, "/default_tablet");
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

    gce.candidates.add("/4/t-0");
    gce.candidates.add("/4/t-0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.candidates.add("/6/t-0");
    gce.candidates.add("hdfs://foo:6000/accumulo/tables/7/t-0/");

    gce.addDirReference("4", null, "/t-0");
    gce.addDirReference("7", null, "hdfs://foo.com:6000/accumulo/tables/7/t-0");

    gca.collect(gce);

    HashSet<String> tids = new HashSet<>();
    tids.add("5");
    tids.add("6");

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

  @Test
  public void testMissingTableIds() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");

    gce.addFileReference("a", null, "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.addFileReference("c", null, "hdfs://foo.com:6000/user/foo/tables/c/t-0/F00.rf");

    // add 2 more table references that will not be seen by in the scan
    gce.tableIds.addAll(Arrays.asList("b", "d"));

    String msg = assertThrows(RuntimeException.class, () -> gca.collect(gce)).getMessage();
    assertTrue(msg, (msg.contains("[b, d]") || msg.contains("[d, b]"))
        && msg.contains("Saw table IDs in ZK that were not in metadata table:"));
  }

  // below are tests for potential failure conditions of the GC process. Some of these cases were
  // observed on clusters. Some were hypothesis based on observations. The result was that
  // candidate entries were not removed when they should have been and therefore files were
  // removed from HDFS that were actually still in use

  private Set<String> makeUnmodifiableSet(String... args) {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(args)));
  }

  @Test
  public void testNormalGCRun() {
    // happy path, no tables added or removed during this portion and all the tables checked
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<String> tablesSeen = makeUnmodifiableSet("2", "1", "3");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "3", "2");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  @Test
  public void testTableAddedInMiddle() {
    // table was added during this portion and we don't see it, should be fine
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<String> tablesSeen = makeUnmodifiableSet("2", "1", "3");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "3", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  @Test
  public void testTableAddedInMiddleTwo() {
    // table was added during this portion and we DO see it
    // Means table was added after candidates were grabbed, so there should be nothing to remove
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<String> tablesSeen = makeUnmodifiableSet("2", "1", "3", "4");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "3", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  @Test
  public void testTableDeletedInMiddle() {
    // table was deleted during this portion and we don't see it
    // this mean any candidates from the deleted table wil stay on the candidate list
    // and during the delete step they will try to removed
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<String> tablesSeen = makeUnmodifiableSet("2", "1", "4");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  @Test
  public void testTableDeletedInMiddleTwo() {
    // table was deleted during this portion and we DO see it
    // this mean candidates from the deleted table may get removed from the candidate list
    // which should be ok, as the delete table function should be responsible for removing those
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<String> tablesSeen = makeUnmodifiableSet("2", "1", "4", "3");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "2", "4");

    new GarbageCollectionAlgorithm().ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter);
  }

  @Test
  public void testMissEntireTable() {
    // this test simulates missing an entire table when looking for what files are in use
    // if you add custom splits to the metadata at able boundaries, this can happen with a failed
    // scan
    // recall the ~tab:~pr for this first entry of a new table is empty, so there is now way to
    // check the prior row. If you split a couple of tables in the metadata the table boundary
    // , say table ids 2,3,4, and then miss scanning table 3 but get 4, it is possible other
    // consistency checks will miss this
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<String> tablesSeen = makeUnmodifiableSet("1", "2", "4");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "2", "3", "4");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.startsWith("Saw table IDs in ZK that were not in metadata table:"));

  }

  @Test
  public void testZKHadNoTables() {
    // this test simulates getting nothing from ZK for table ids, which should not happen,
    // but just in case let's test
    Set<String> tablesBefore = makeUnmodifiableSet();
    Set<String> tablesSeen = makeUnmodifiableSet("1", "2");
    Set<String> tablesAfter = makeUnmodifiableSet();

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.startsWith("Saw no table ids in ZK but did see table ids in metadata table:"));
  }

  @Test
  public void testMissingTableAndTableAdd() {
    // simulates missing a table when checking references, and a table being added
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3");
    Set<String> tablesSeen = makeUnmodifiableSet("1", "2", "4");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "2", "3", "4");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.equals("Saw table IDs in ZK that were not in metadata table:  [3]"));
  }

  @Test
  public void testMissingTableAndTableDeleted() {
    // simulates missing a table when checking references, and a table being deleted
    Set<String> tablesBefore = makeUnmodifiableSet("1", "2", "3", "4");
    Set<String> tablesSeen = makeUnmodifiableSet("1", "2", "4");
    Set<String> tablesAfter = makeUnmodifiableSet("1", "2", "3");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    String msg = assertThrows(RuntimeException.class,
        () -> gca.ensureAllTablesChecked(tablesBefore, tablesSeen, tablesAfter)).getMessage();
    assertTrue(msg.equals("Saw table IDs in ZK that were not in metadata table:  [3]"));

  }
}
