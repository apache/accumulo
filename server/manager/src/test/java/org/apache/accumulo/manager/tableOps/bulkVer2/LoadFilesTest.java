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

import org.apache.accumulo.core.clientImpl.bulk.Bulk.FileInfo;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.bulkVer2.LoadFiles.TabletsMetadataFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
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

  @Test
  public void testFindOverlappingFiles() {

    TableId tid = TableId.of("1a");
    List<TabletMetadata> tm = new ArrayList<>();
    tm.add(TabletMetadata.create(tid.canonical(), null, "a"));
    for (int i = 97; i < 122; i++) {
      tm.add(TabletMetadata.create(tid.canonical(), "" + (char) (i), "" + (char) (i + 1)));
    }
    tm.add(TabletMetadata.create(tid.canonical(), "z", null));

    List<TabletMetadata> tablets =
        LoadFiles.findOverlappingTablets(new KeyExtent(tid, new Text("c"), null), tm.iterator());
    assertEquals(3, tablets.size());
    assertEquals(tm.get(0), tablets.get(0));
    assertEquals(tm.get(1), tablets.get(1));
    assertEquals(tm.get(2), tablets.get(2));

    tablets =
        LoadFiles.findOverlappingTablets(new KeyExtent(tid, null, new Text("x")), tm.iterator());
    assertEquals(3, tablets.size());
    assertEquals(tm.get(tm.size() - 3), tablets.get(tablets.size() - 3));
    assertEquals(tm.get(tm.size() - 2), tablets.get(tablets.size() - 2));
    assertEquals(tm.get(tm.size() - 1), tablets.get(tablets.size() - 1));

    tablets = LoadFiles.findOverlappingTablets(new KeyExtent(tid, null, null), tm.iterator());
    assertEquals(tm, tablets);

  }

  @Test
  public void testLoadFiles() throws Exception {

    TableId tid = TableId.of("1");
    List<TabletMetadata> tm = new ArrayList<>();
    tm.add(TabletMetadata.create(tid.canonical(), null, "a"));
    for (int i = 97; i < 122; i++) {
      tm.add(TabletMetadata.create(tid.canonical(), "" + (char) (i), "" + (char) (i + 1)));
    }
    tm.add(TabletMetadata.create(tid.canonical(), "z", null));

    TabletsMetadata tabletMeta = new TestTabletsMetadata(null, tm);

    Map<KeyExtent,String> loadRanges = new HashMap<>();
    loadRanges.put(nke(null, "c"), "f1 f2");
    loadRanges.put(nke("c", "g"), "f2 f3");
    loadRanges.put(nke("g", "r"), "f2 f4");
    loadRanges.put(nke("r", "w"), "f2 f5");
    loadRanges.put(nke("w", null), "f2 f6");
    LoadMappingIterator lmi = PrepBulkImportTest.createLoadMappingIter(loadRanges);

    CaptureLoader cl = new CaptureLoader();
    BulkInfo info = new BulkInfo();
    TabletsMetadataFactory tmf = new TabletsMetadataFactory() {

      @Override
      public TabletsMetadata newTabletsMetadata(Text startRow) {
        return tabletMeta;
      }

      @Override
      public void close() {
        tabletMeta.close();
      }

    };
    long txid = 1234L;

    Manager manager = EasyMock.createMock(Manager.class);
    Path bulkDir = EasyMock.createMock(Path.class);
    EasyMock.replay(manager, bulkDir);

    LoadFiles.loadFiles(cl, info, bulkDir, lmi, tmf, manager, txid);
    EasyMock.verify(manager, bulkDir);

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
    HashSet<KeyExtent> extents = loadFileToExtentMap.get("f1");
    assertNotNull(extents);
    assertEquals(3, extents.size());
    assertTrue(extents.contains(nke(null, "a")));
    assertTrue(extents.contains(nke("a", "b")));
    assertTrue(extents.contains(nke("b", "c")));

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
    assertEquals(5, extents.size());
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
