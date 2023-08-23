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

    public TestGCE() {}

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

    public void addFileReference(String tableId, String endRow, String file) {
      Key key = newFileReferenceKey(tableId, endRow, file);
      Value val = new Value(new DataFileValue(0, 0).encode());
      references.put(key, val);
    }

    public void removeFileReference(String tableId, String endRow, String file) {
      references.remove(newFileReferenceKey(tableId, endRow, file));
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

    public void addDirReference(String tableId, String endRow, String dir) {
      Key key = newDirReferenceKey(tableId, endRow);
      Value val = new Value(dir.getBytes());
      references.put(key, val);
    }

    public Key newPrevRowKey(String tableId, String endRow) {
      String row = new KeyExtent(tableId, endRow == null ? null : new Text(endRow), null)
          .getMetadataEntry().toString();
      String cf = MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.getColumnFamily()
          .toString();
      String cq = MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN
          .getColumnQualifier().toString();
      return new Key(row, cf, cq);
    }

    public void addPrevRowReference(String tableId, String endRow) {
      Key key = newPrevRowKey(tableId, endRow);
      Value val = new Value();
      references.put(key, val);
    }

    public void removeDirReference(String tableId, String endRow) {
      references.remove(newDirReferenceKey(tableId, endRow));
    }

    public void removePrevRowReference(String tableId, String endRow) {
      references.remove(newPrevRowKey(tableId, endRow));
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

    gce.addDirReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0");
    gce.addPrevRowReference("4", null);
    gce.addDirReference("5", null, "hdfs://foo.com:6000/accumulo/tables/4/t0");
    gce.addPrevRowReference("5", null);

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
    gce.addDirReference("4", null, "hdfs://foo.com:6000/accumulo/tables/4/t0");
    gce.addPrevRowReference("4", null);

    gce.addFileReference("5", null, "../4/t0/F000.rf");
    gce.addDirReference("5", null, "hdfs://foo.com:6000/accumulo/tables/4/t0");
    gce.addPrevRowReference("5", null);

    gce.addFileReference("6", null, "hdfs://foo.com:6000/accumulo/tables/4/t0/F000.rf");
    gce.addDirReference("6", null, "hdfs://foo.com:6000/accumulo/tables/4/t0");
    gce.addPrevRowReference("6", null);

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
    gce.addPrevRowReference("4", null);
    gce.addDirReference("5", null, "hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.addPrevRowReference("5", null);
    gce.addDirReference("6", null, "hdfs://foo.com:6000/accumulo/tables/6/t-0");
    gce.addPrevRowReference("6", null);
    gce.addDirReference("7", null, "hdfs://foo.com:6000/accumulo/tables/7/t-0");
    gce.addPrevRowReference("7", null);

    gce.addFileReference("8", "m", "/t-0/F00.rf");
    gce.addDirReference("8", "m", "/t-0");
    gce.addPrevRowReference("8", "m");

    gce.addFileReference("9", "m", "/t-0/F00.rf");
    gce.addDirReference("9", "m", "/t-0");
    gce.addPrevRowReference("9", "m");

    gce.addFileReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-0/F00.rf");
    gce.addDirReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-0");
    gce.addPrevRowReference("a", "m");

    gce.addFileReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-0/F00.rf");
    gce.addDirReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-0");
    gce.addPrevRowReference("b", "m");

    gce.addFileReference("e", "m", "../c/t-0/F00.rf");
    gce.addDirReference("e", "m", "hdfs://foo.com:6000/accumulo/tables/c/t-0");
    gce.addPrevRowReference("e", "m");

    gce.addFileReference("f", "m", "../d/t-0/F00.rf");
    gce.addDirReference("f", "m", "hdfs://foo.com:6000/accumulo/tables/d/t-0");
    gce.addPrevRowReference("f", "m");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // A directory reference does not preclude a candidate file beneath that directory from deletion
    gca.collect(gce);
    assertRemoved(gce, "/4/t-0/F002.rf");

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    gce.removePrevRowReference("5", null);

    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/accumulo/tables/5/t-0");

    gce.removeDirReference("4", null);
    gce.removePrevRowReference("4", null);

    gca.collect(gce);
    assertRemoved(gce, "/4/t-0");

    gce.removeDirReference("6", null);
    gce.removePrevRowReference("6", null);
    gce.removeDirReference("7", null);
    gce.removePrevRowReference("7", null);

    gca.collect(gce);
    assertRemoved(gce, "/6/t-0", "hdfs://foo:6000/accumulo/tables/7/t-0/");

    gce.removeFileReference("8", "m", "/t-0/F00.rf");
    gce.removeDirReference("8", "m");
    gce.removePrevRowReference("8", "m");

    gce.removeFileReference("9", "m", "/t-0/F00.rf");
    gce.removeDirReference("9", "m");
    gce.removePrevRowReference("9", "m");

    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/accumulo/tables/a/t-0/F00.rf");
    gce.removeDirReference("a", "m");
    gce.removePrevRowReference("a", "m");

    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/accumulo/tables/b/t-0/F00.rf");
    gce.removeDirReference("b", "m");
    gce.removePrevRowReference("b", "m");

    gce.removeFileReference("e", "m", "../c/t-0/F00.rf");
    gce.removeDirReference("e", "m");
    gce.removePrevRowReference("e", "m");

    gce.removeFileReference("f", "m", "../d/t-0/F00.rf");
    gce.removeDirReference("f", "m");
    gce.removePrevRowReference("f", "m");

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
    gce.addPrevRowReference("4", null);
    gce.addDirReference("5", null, "/t-0");
    gce.addPrevRowReference("5", null);
    gce.addDirReference("6", null, "hdfs://foo.com:6000/user/foo/tables/6/t-0");
    gce.addPrevRowReference("6", null);
    gce.addDirReference("7", null, "hdfs://foo.com:6000/user/foo/tables/7/t-0");
    gce.addPrevRowReference("7", null);

    gce.addFileReference("8", "m", "/t-0/F00.rf");
    gce.addDirReference("8", "m", "hdfs://foo.com:6000/user/foo/tables/8/t-0");
    gce.addPrevRowReference("8", "m");

    gce.addFileReference("9", "m", "/t-0/F00.rf");
    gce.addDirReference("9", "m", "hdfs://foo.com:6000/user/foo/tables/9/t-0");
    gce.addPrevRowReference("9", "m");

    gce.addFileReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.addDirReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-0");
    gce.addPrevRowReference("a", "m");

    gce.addFileReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-0/F00.rf");
    gce.addDirReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-0");
    gce.addPrevRowReference("b", "m");

    gce.addFileReference("e", "m", "../c/t-0/F00.rf");
    gce.addDirReference("e", "m", "hdfs://foo.com:6000/user/foo/tables/c/t-0");
    gce.addPrevRowReference("e", "m");

    gce.addFileReference("f", "m", "../d/t-0/F00.rf");
    gce.addDirReference("f", "m", "hdfs://foo.com:6000/user/foo/tables/d/t-0");
    gce.addPrevRowReference("f", "m");

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    // A directory reference does not preclude a candidate file beneath that directory from deletion
    gca.collect(gce);
    assertRemoved(gce, "/4/t-0/F002.rf");

    // Removing the dir reference for a table will delete all tablet directories
    gce.removeDirReference("5", null);
    gce.removePrevRowReference("5", null);
    gca.collect(gce);
    assertRemoved(gce, "hdfs://foo.com:6000/user/foo/tables/5/t-0");

    gce.removeDirReference("4", null);
    gce.removePrevRowReference("4", null);
    gca.collect(gce);
    assertRemoved(gce, "/4/t-0");

    gce.removeDirReference("6", null);
    gce.removePrevRowReference("6", null);

    gce.removeDirReference("7", null);
    gce.removePrevRowReference("7", null);

    gca.collect(gce);
    assertRemoved(gce, "/6/t-0", "hdfs://foo:6000/user/foo/tables/7/t-0/");

    gce.removeFileReference("8", "m", "/t-0/F00.rf");
    gce.removeDirReference("8", "m");
    gce.removePrevRowReference("8", "m");

    gce.removeFileReference("9", "m", "/t-0/F00.rf");
    gce.removeDirReference("9", "m");
    gce.removePrevRowReference("9", "m");

    gce.removeFileReference("a", "m", "hdfs://foo.com:6000/user/foo/tables/a/t-0/F00.rf");
    gce.removeDirReference("a", "m");
    gce.removePrevRowReference("a", "m");

    gce.removeFileReference("b", "m", "hdfs://foo.com:6000/user/foo/tables/b/t-0/F00.rf");
    gce.removeDirReference("b", "m");
    gce.removePrevRowReference("b", "m");

    gce.removeFileReference("e", "m", "../c/t-0/F00.rf");
    gce.removeDirReference("e", "m");
    gce.removePrevRowReference("e", "m");

    gce.removeFileReference("f", "m", "../d/t-0/F00.rf");
    gce.removeDirReference("f", "m");
    gce.removePrevRowReference("f", "m");

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
    gce.addPrevRowReference("1636", null);
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
    gce.addPrevRowReference("1636", null);
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

    gce.tableIds.add("4");

    gce.candidates.add("/4/t-0");
    gce.candidates.add("/4/t-0/F002.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t-0");
    gce.candidates.add("/6/t-0");
    gce.candidates.add("hdfs://foo:6000/accumulo/tables/7/t-0/");

    gce.addDirReference("7", null, "hdfs://foo.com:6000/accumulo/tables/7/t-0");
    gce.addPrevRowReference("7", null);

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

  /**
   * Minimal test to show that dir and prevRow are required for valid scan (go path)
   */
  @Test
  public void testDirAndPrevRow() throws Exception {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();
    gce.candidates.add("/1636/default_tablet");
    gce.addDirReference("1636", null, "/default_tablet");
    gce.addPrevRowReference("1636", null);
    gca.collect(gce);
    assertEquals(0, gce.deletes.size());
  }

  /**
   * Show that IllegalState is thrown when no dir entry present in metadata scan in last row seen.
   */
  @Test
  public void testNoDirAsLastRow() {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();
    gce.candidates.add("/1636/default_tablet");
    gce.addPrevRowReference("1636", null);
    assertThrows(IllegalStateException.class, () -> gca.collect(gce));
    assertEquals(1, gce.candidates.size());
  }

  /**
   * Show that IllegalState is thrown when no dir entry present in metadata scan.
   */
  @Test
  public void testNoDir() {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();
    gce.candidates.add("/1636/default_tablet/f1");
    gce.addPrevRowReference("1636", "a");

    gce.candidates.add("/1636/t1/f2");
    gce.addDirReference("1636", null, "/t1");
    gce.addPrevRowReference("1636", null);

    assertThrows(IllegalStateException.class, () -> gca.collect(gce));
    assertEquals(2, gce.candidates.size());
  }

  /**
   * Show that IllegalState is thrown when no prev row present in metadata scan in last row seen.
   */
  @Test
  public void testNoPrevRowAsLastRow() {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();
    gce.candidates.add("/1636/default_tablet");
    gce.addDirReference("1636", null, "/default_tablet");
    assertThrows(IllegalStateException.class, () -> gca.collect(gce));
    assertEquals(1, gce.candidates.size());
  }

  /**
   * Show that IllegalState is thrown when no prevRow entry present in metadata scan.
   */
  @Test
  public void testPrevRow() {
    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();

    TestGCE gce = new TestGCE();
    gce.candidates.add("/1636/default_tablet/f1");
    gce.addDirReference("1636", "a", "/default_tablet");

    gce.candidates.add("/1636/t1/f2");
    gce.addDirReference("1636", null, "/t1");
    gce.addPrevRowReference("1636", null);

    assertThrows(IllegalStateException.class, () -> gca.collect(gce));
    assertEquals(2, gce.candidates.size());
  }

  /**
   * Show that IllegalState is thrown when no prevRow entry present in metadata scan.
   */
  @Test
  public void testPrevRowOnly() {
    TestGCE gce = new TestGCE();

    gce.candidates.add("hdfs://foo:6000/accumulo/tables/4/t0/F000.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/4/t0/F001.rf");
    gce.candidates.add("hdfs://foo.com:6000/accumulo/tables/5/t0/F005.rf");

    gce.addPrevRowReference("1636", null);

    GarbageCollectionAlgorithm gca = new GarbageCollectionAlgorithm();
    assertThrows(IllegalStateException.class, () -> gca.collect(gce));
    assertEquals(3, gce.candidates.size());
  }

}
