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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.bulk.Bulk.FileInfo;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.bulkVer2.LoadFiles.ImportTimingStats;
import org.apache.accumulo.manager.tableOps.bulkVer2.LoadFiles.TabletsMetadataFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
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

    public CaptureLoader(Manager manager, TableId tableId) {
      super(manager, tableId);
    }

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
    void start(Path bulkDir, Manager manager, TableId tableId, FateId fateId, boolean setTime)
        throws Exception {
      // override to do nothing
    }

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

    String fmtTid = FateId.from(FateInstanceType.USER, UUID.randomUUID()).canonical();
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

    Manager manager = EasyMock.createMock(Manager.class);
    ServerContext ctx = EasyMock.createMock(ServerContext.class);
    TableConfiguration tconf = EasyMock.createMock(TableConfiguration.class);

    EasyMock.expect(manager.getContext()).andReturn(ctx).anyTimes();
    EasyMock.expect(ctx.getTableConfiguration(tid)).andReturn(tconf).anyTimes();
    EasyMock.expect(tconf.getCount(Property.TABLE_FILE_PAUSE))
        .andReturn(Integer.parseInt(Property.TABLE_FILE_PAUSE.getDefaultValue())).anyTimes();

    Path bulkDir = EasyMock.createMock(Path.class);
    EasyMock.replay(manager, ctx, tconf, bulkDir);

    TabletsMetadata tabletMeta = new TestTabletsMetadata(null, tm);
    LoadMappingIterator lmi = PrepBulkImportTest.createLoadMappingIter(loadRanges);
    CaptureLoader cl = new CaptureLoader(manager, tid);
    BulkInfo info = new BulkInfo();
    TabletsMetadataFactory tmf = (startRow) -> tabletMeta;
    FateId txid = FateId.from(FateInstanceType.USER, UUID.randomUUID());

    LoadFiles.loadFiles(cl, info, bulkDir, lmi, tmf, manager, txid, 0);
    EasyMock.verify(manager, ctx, tconf, bulkDir);
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
}
