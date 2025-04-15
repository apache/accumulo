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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import static org.apache.accumulo.manager.tableOps.bulkVer2.PrepBulkImportTest.nke;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.bulk.Bulk.FileInfo;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.bulkVer2.LoadFiles.ImportTimingStats;
import org.apache.accumulo.manager.tableOps.bulkVer2.LoadFiles.TabletsMetadataFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LoadFilesTest {

  public static class TestTabletsMetadata extends TabletsMetadata {

    public TestTabletsMetadata(AutoCloseable closeable, Iterable<TabletMetadata> tmi) {
      super(closeable, tmi);
    }

  }

  private static class CaptureLoader extends LoadFiles.Loader {

    private static class LoadResult {
      private final List<TabletMetadata> tablets;
      private final Files files;

      public LoadResult(List<TabletMetadata> tablets, Files files) {
        super();
        this.tablets = tablets;
        this.files = files;
      }

      public List<TabletMetadata> getTablets() {
        return tablets;
      }

      public Files getFiles() {
        return files;
      }

    }

    private final List<LoadResult> results = new ArrayList<>();

    @Override
    void load(List<TabletMetadata> tablets, Files files) {
      results.add(new LoadResult(tablets, files));
    }

    public List<LoadResult> getLoadResults() {
      return results;
    }

    @Override
    long finish() {
      return 0;
    }

  }

  private TableId tid = TableId.of("1");
  private List<TabletMetadata> tm = new ArrayList<>();

  @BeforeEach
  public void setup() {
    tm.clear();
    tm.add(TabletMetadata.create(tid.canonical(), null, "a"));
    for (int i = 'a'; i < 'z'; i++) {
      tm.add(TabletMetadata.create(tid.canonical(), "" + (char) (i), "" + (char) (i + 1)));
    }
    tm.add(TabletMetadata.create(tid.canonical(), "z", null));
  }

  @Test
  public void testFindOverlappingFiles() {

    String fmtTid = FateTxId.formatTid(1234L);
    var iter = tm.iterator();
    List<TabletMetadata> tablets = LoadFiles.findOverlappingTablets(fmtTid,
        new KeyExtent(tid, new Text("c"), null), iter, new ImportTimingStats());
    assertEquals(tm.get(3), iter.next());
    assertEquals(3, tablets.size());
    assertEquals(tm.get(0), tablets.get(0));
    assertEquals(tm.get(1), tablets.get(1));
    assertEquals(tm.get(2), tablets.get(2));

    iter = tm.iterator();
    tablets = LoadFiles.findOverlappingTablets(fmtTid,
        new KeyExtent(tid, new Text("o"), new Text("j")), iter, new ImportTimingStats());
    assertEquals(tm.get(tm.size() - 12), iter.next());
    assertEquals(5, tablets.size());
    assertEquals(tm.get(tm.size() - 17), tablets.get(tablets.size() - 5));
    assertEquals(tm.get(tm.size() - 16), tablets.get(tablets.size() - 4));
    assertEquals(tm.get(tm.size() - 15), tablets.get(tablets.size() - 3));
    assertEquals(tm.get(tm.size() - 14), tablets.get(tablets.size() - 2));
    assertEquals(tm.get(tm.size() - 13), tablets.get(tablets.size() - 1));

    iter = tm.iterator();
    tablets = LoadFiles.findOverlappingTablets(fmtTid, new KeyExtent(tid, null, new Text("x")),
        iter, new ImportTimingStats());
    assertEquals(3, tablets.size());
    assertEquals(tm.get(tm.size() - 3), tablets.get(tablets.size() - 3));
    assertEquals(tm.get(tm.size() - 2), tablets.get(tablets.size() - 2));
    assertEquals(tm.get(tm.size() - 1), tablets.get(tablets.size() - 1));
    assertTrue(!iter.hasNext());

    tablets = LoadFiles.findOverlappingTablets(fmtTid, new KeyExtent(tid, null, null),
        tm.iterator(), new ImportTimingStats());
    assertEquals(tm, tablets);

  }

  private Map<String,HashSet<KeyExtent>> runLoadFilesLoad(Map<KeyExtent,String> loadRanges)
      throws Exception {

    TabletsMetadata tabletMeta = new TestTabletsMetadata(null, tm);
    LoadMappingIterator lmi = PrepBulkImportTest.createLoadMappingIter(loadRanges);
    CaptureLoader cl = new CaptureLoader();
    BulkInfo info = new BulkInfo();
    TabletsMetadataFactory tmf = (startRow) -> tabletMeta;
    long txid = 1234L;

    Manager manager = EasyMock.createMock(Manager.class);
    Path bulkDir = EasyMock.createMock(Path.class);
    replay(manager, bulkDir);

    LoadFiles.loadFiles(cl, info, bulkDir, lmi, tmf, manager, txid, 0);
    verify(manager, bulkDir);
    List<CaptureLoader.LoadResult> results = cl.getLoadResults();
    assertEquals(loadRanges.size(), results.size());

    Map<String,HashSet<KeyExtent>> loadFileToExtentMap = new HashMap<>();
    for (CaptureLoader.LoadResult result : results) {
      for (FileInfo file : result.getFiles()) {
        HashSet<KeyExtent> extents =
            loadFileToExtentMap.computeIfAbsent(file.getFileName(), fileName -> new HashSet<>());
        result.getTablets().forEach(m -> extents.add(m.getExtent()));
      }
    }
    return loadFileToExtentMap;
  }

  @Test
  public void testLoadFilesPartialTable() throws Exception {
    Map<KeyExtent,String> loadRanges = new HashMap<>();
    loadRanges.put(nke("c", "g"), "f2 f3");
    loadRanges.put(nke("l", "n"), "f1 f2 f4 f5");
    loadRanges.put(nke("r", "w"), "f2 f5");

    Map<String,HashSet<KeyExtent>> loadFileToExtentMap = runLoadFilesLoad(loadRanges);
    assertEquals(5, loadFileToExtentMap.size());

    HashSet<KeyExtent> extents = loadFileToExtentMap.get("f1");
    assertNotNull(extents);
    assertEquals(2, extents.size());
    assertTrue(extents.contains(nke("l", "m")));
    assertTrue(extents.contains(nke("m", "n")));

    extents = loadFileToExtentMap.get("f2");
    assertEquals(11, extents.size());
    assertTrue(extents.contains(nke("c", "d")));
    assertTrue(extents.contains(nke("d", "e")));
    assertTrue(extents.contains(nke("e", "f")));
    assertTrue(extents.contains(nke("f", "g")));
    assertTrue(extents.contains(nke("l", "m")));
    assertTrue(extents.contains(nke("m", "n")));
    assertTrue(extents.contains(nke("r", "s")));
    assertTrue(extents.contains(nke("s", "t")));
    assertTrue(extents.contains(nke("t", "u")));
    assertTrue(extents.contains(nke("u", "v")));
    assertTrue(extents.contains(nke("v", "w")));

    extents = loadFileToExtentMap.get("f3");
    assertEquals(4, extents.size());
    assertTrue(extents.contains(nke("c", "d")));
    assertTrue(extents.contains(nke("d", "e")));
    assertTrue(extents.contains(nke("e", "f")));
    assertTrue(extents.contains(nke("f", "g")));

    extents = loadFileToExtentMap.get("f4");
    assertEquals(2, extents.size());
    assertTrue(extents.contains(nke("l", "m")));
    assertTrue(extents.contains(nke("m", "n")));

    extents = loadFileToExtentMap.get("f5");
    assertEquals(7, extents.size());
    assertTrue(extents.contains(nke("l", "m")));
    assertTrue(extents.contains(nke("m", "n")));
    assertTrue(extents.contains(nke("r", "s")));
    assertTrue(extents.contains(nke("s", "t")));
    assertTrue(extents.contains(nke("t", "u")));
    assertTrue(extents.contains(nke("u", "v")));
    assertTrue(extents.contains(nke("v", "w")));

  }

  @Test
  public void testLoadFilesEntireTable() throws Exception {

    Map<KeyExtent,String> loadRanges = new HashMap<>();
    loadRanges.put(nke(null, "c"), "f1 f2");
    loadRanges.put(nke("c", "g"), "f2 f3");
    loadRanges.put(nke("g", "l"), "f2 f4");
    loadRanges.put(nke("l", "n"), "f1 f2 f4 f5");
    loadRanges.put(nke("n", "r"), "f2 f4");
    loadRanges.put(nke("r", "w"), "f2 f5");
    loadRanges.put(nke("w", null), "f2 f6");

    Map<String,HashSet<KeyExtent>> loadFileToExtentMap = runLoadFilesLoad(loadRanges);
    assertEquals(6, loadFileToExtentMap.size());

    HashSet<KeyExtent> extents = loadFileToExtentMap.get("f1");
    assertNotNull(extents);
    assertEquals(5, extents.size());
    assertTrue(extents.contains(nke(null, "a")));
    assertTrue(extents.contains(nke("a", "b")));
    assertTrue(extents.contains(nke("b", "c")));
    assertTrue(extents.contains(nke("l", "m")));
    assertTrue(extents.contains(nke("m", "n")));

    extents = loadFileToExtentMap.get("f2");
    assertEquals(tm.size(), extents.size());
    for (TabletMetadata m : tm) {
      assertTrue(extents.contains(m.getExtent()));
    }

    extents = loadFileToExtentMap.get("f3");
    assertEquals(4, extents.size());
    assertTrue(extents.contains(nke("c", "d")));
    assertTrue(extents.contains(nke("d", "e")));
    assertTrue(extents.contains(nke("e", "f")));
    assertTrue(extents.contains(nke("f", "g")));

    extents = loadFileToExtentMap.get("f4");
    assertEquals(11, extents.size());
    assertTrue(extents.contains(nke("g", "h")));
    assertTrue(extents.contains(nke("h", "i")));
    assertTrue(extents.contains(nke("i", "j")));
    assertTrue(extents.contains(nke("j", "k")));
    assertTrue(extents.contains(nke("k", "l")));
    assertTrue(extents.contains(nke("l", "m")));
    assertTrue(extents.contains(nke("m", "n")));
    assertTrue(extents.contains(nke("n", "o")));
    assertTrue(extents.contains(nke("o", "p")));
    assertTrue(extents.contains(nke("p", "q")));
    assertTrue(extents.contains(nke("q", "r")));

    extents = loadFileToExtentMap.get("f5");
    assertEquals(7, extents.size());
    assertTrue(extents.contains(nke("l", "m")));
    assertTrue(extents.contains(nke("m", "n")));
    assertTrue(extents.contains(nke("r", "s")));
    assertTrue(extents.contains(nke("s", "t")));
    assertTrue(extents.contains(nke("t", "u")));
    assertTrue(extents.contains(nke("u", "v")));
    assertTrue(extents.contains(nke("v", "w")));

    extents = loadFileToExtentMap.get("f6");
    assertEquals(4, extents.size());
    assertTrue(extents.contains(nke("w", "x")));
    assertTrue(extents.contains(nke("x", "y")));
    assertTrue(extents.contains(nke("y", "z")));
    assertTrue(extents.contains(nke("z", null)));

  }

  /**
   * Test tablets without locations that have loaded files and do not have loaded files.
   *
   */
  @Test
  public void testLoadLocation() throws Exception {

    var loader = new LoadFiles.OnlineLoader(DefaultConfiguration.getInstance()) {
      @Override
      protected void addToQueue(HostAndPort server, KeyExtent extent,
          Map<String,MapFileInfo> thriftImports) {
        fail();
      }
    };

    Path bulkDir = new Path("file:/accumulo/tables/1/b-00001");

    Path fullPath = new Path(bulkDir, "f1.rf");
    TabletFile loaded1 = new TabletFile(fullPath);

    // Tablet with no location and no loaded files
    TabletMetadata tablet1 = createMock(TabletMetadata.class);
    expect(tablet1.getLocation()).andReturn(null).once();
    expect(tablet1.getLoaded()).andReturn(Map.of()).once();

    // Tablet with no location and loaded files
    TabletMetadata tablet2 = createMock(TabletMetadata.class);
    expect(tablet2.getLocation()).andReturn(null).once();
    expect(tablet2.getLoaded()).andReturn(Map.of(loaded1, 123456789L)).once();

    Manager manager = strictMock(Manager.class);
    expect(manager.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).once();

    replay(tablet1, tablet2, manager);

    loader.start(bulkDir, manager, 123456789L, false);

    Files files = new Files(List.of(new FileInfo("f1.rf", 50, 50)));

    // Since this tablet already has loaded the files the locationLess counter should not be
    // incremented
    loader.load(List.of(tablet2), files);
    assertEquals(0, loader.locationLess);

    // Since this tablet has not loaded files the locationLess counter should be incremented
    loader.load(List.of(tablet1), files);
    assertEquals(1, loader.locationLess);

    verify(tablet1, tablet2, manager);
  }
}
