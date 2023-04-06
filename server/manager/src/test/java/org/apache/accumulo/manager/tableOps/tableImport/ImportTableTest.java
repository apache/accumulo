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
package org.apache.accumulo.manager.tableOps.tableImport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class ImportTableTest {

  @Test
  public void testParseExportDir() {
    List<ImportedTableInfo.DirectoryMapping> out;

    // null
    out = ImportTable.parseExportDir(null);
    assertEquals(0, out.size());

    // empty
    out = ImportTable.parseExportDir(Set.of(""));
    assertEquals(0, out.size());

    // single
    out = ImportTable.parseExportDir(Set.of("hdfs://nn1:8020/apps/import"));
    assertEquals(1, out.size());
    assertEquals("hdfs://nn1:8020/apps/import", out.get(0).exportDir);
    assertNull(out.get(0).importDir);

    // multiple
    out = ImportTable
        .parseExportDir(Set.of("hdfs://nn1:8020/apps/import", "hdfs://nn2:8020/apps/import"));
    assertEquals(2, out.size());

    out = out.stream().filter(i -> i.importDir == null).collect(Collectors.toList());
    assertEquals(2, out.size());

    out = out.stream().filter(i -> !i.exportDir.equals("hdfs://nn1:8020/apps/import"))
        .filter(i -> !i.exportDir.equals("hdfs://nn2:8020/apps/import"))
        .collect(Collectors.toList());
    assertEquals(0, out.size());
  }

  @Test
  public void testCreateImportDir() throws Exception {
    Manager manager = EasyMock.createMock(Manager.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager volumeManager = EasyMock.createMock(VolumeManager.class);
    UniqueNameAllocator uniqueNameAllocator = EasyMock.createMock(UniqueNameAllocator.class);

    String[] expDirs = {"hdfs://nn1:8020/import-dir-nn1", "hdfs://nn2:8020/import-dir-nn2",
        "hdfs://nn3:8020/import-dir-nn3"};
    String[] tableDirs =
        {"hdfs://nn1:8020/apps/accumulo1/tables", "hdfs://nn2:8020/applications/accumulo/tables",
            "hdfs://nn3:8020/applications/accumulo/tables"};

    Set<String> tableDirSet = Set.of(tableDirs);

    String dirName = "abcd";

    EasyMock.expect(manager.getContext()).andReturn(context);
    EasyMock.expect(manager.getVolumeManager()).andReturn(volumeManager).times(3);
    EasyMock.expect(context.getUniqueNameAllocator()).andReturn(uniqueNameAllocator);
    EasyMock.expect(volumeManager.matchingFileSystem(EasyMock.eq(new Path(expDirs[0])),
        EasyMock.eq(tableDirSet))).andReturn(new Path(tableDirs[0]));
    EasyMock.expect(volumeManager.matchingFileSystem(EasyMock.eq(new Path(expDirs[1])),
        EasyMock.eq(tableDirSet))).andReturn(new Path(tableDirs[1]));
    EasyMock.expect(volumeManager.matchingFileSystem(EasyMock.eq(new Path(expDirs[2])),
        EasyMock.eq(tableDirSet))).andReturn(new Path(tableDirs[2]));
    EasyMock.expect(uniqueNameAllocator.getNextName()).andReturn(dirName).times(3);

    ImportedTableInfo ti = new ImportedTableInfo();
    ti.tableId = TableId.of("5b");
    ti.directories = ImportTable.parseExportDir(Set.of(expDirs));
    assertEquals(3, ti.directories.size());

    EasyMock.replay(manager, context, volumeManager, uniqueNameAllocator);

    CreateImportDir ci = new CreateImportDir(ti);
    ci.create(tableDirSet, manager);
    assertEquals(3, ti.directories.size());
    for (ImportedTableInfo.DirectoryMapping dm : ti.directories) {
      assertNotNull(dm.exportDir);
      assertNotNull(dm.importDir);
      assertTrue(dm.importDir.contains(Constants.HDFS_TABLES_DIR));
      assertMatchingFilesystem(dm.exportDir, dm.importDir);
      assertTrue(
          dm.importDir.contains(ti.tableId.canonical() + "/" + Constants.BULK_PREFIX + dirName));
    }
    EasyMock.verify(manager, context, volumeManager, uniqueNameAllocator);
  }

  private static void assertMatchingFilesystem(String expected, String target) {
    URI uri1 = URI.create(expected);
    URI uri2 = URI.create(target);

    if (uri1.getScheme().equals(uri2.getScheme())) {
      String a1 = uri1.getAuthority();
      String a2 = uri2.getAuthority();
      if ((a1 == null && a2 == null) || (a1 != null && a1.equals(a2))) {
        return;
      }
    }

    fail("Filesystems do not match: " + expected + " vs. " + target);
  }
}
