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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class UpgradeUtilTest {

  private static final String ZK_ROOT = "/accumulo/test-instance";
  private static final KeyExtent METADATA_EXTENT = new KeyExtent(MetadataTable.ID, null, null);
  private static final KeyExtent LOGGED_USER_EXTENT = new KeyExtent(TableId.of("5"), null, null);
  private static final KeyExtent CLEAN_USER_EXTENT = new KeyExtent(TableId.of("6"), null, null);

  @TempDir
  private java.nio.file.Path tempDir;

  @Test
  public void findsLoggedTabletsAcrossAllMetadataLevels() throws Exception {
    TestData data = createTestData(true, true, true);

    assertEquals(List.of(RootTable.EXTENT, METADATA_EXTENT, LOGGED_USER_EXTENT),
        findRecoveries(data.rootMetadata));
  }

  @Test
  public void returnsEmptyWhenNoTabletsHaveLogs() throws Exception {
    TestData data = createTestData(false, false, false);

    assertEquals(List.of(), findRecoveries(data.rootMetadata));
  }

  private List<KeyExtent> findRecoveries(RootTabletMetadata rootMetadata) throws IOException {
    ZooCache cache = EasyMock.createMock(ZooCache.class);
    EasyMock.expect(cache.get(ZK_ROOT + RootTable.ZROOT_TABLET))
        .andReturn(rootMetadata.toJson().getBytes(UTF_8));
    EasyMock.replay(cache);

    List<KeyExtent> recoveries = new UpgradeUtil().findTabletsNeedingRecovery(ZK_ROOT, cache);
    EasyMock.verify(cache);
    return recoveries;
  }

  private TestData createTestData(boolean rootHasLogs, boolean metadataHasLogs, boolean userHasLogs)
      throws Exception {
    String userRFile = createRFile(TableId.of("5"), "Fuser.rf", new TreeMap<>());

    SortedMap<Key,Value> metadataEntries = new TreeMap<>();
    addTabletMetadata(metadataEntries, LOGGED_USER_EXTENT, userRFile, userHasLogs);
    addTabletMetadata(metadataEntries, CLEAN_USER_EXTENT, null, false);
    String metadataRFile = createRFile(MetadataTable.ID, "Fmetadata.rf", metadataEntries);

    SortedMap<Key,Value> rootEntries = new TreeMap<>();
    addTabletMetadata(rootEntries, METADATA_EXTENT, metadataRFile, metadataHasLogs);
    String rootRFile = createRFile(RootTable.ID, "Froot.rf", rootEntries);

    RootTabletMetadata rootMetadata = new RootTabletMetadata();
    Mutation rootMutation = new Mutation(RootTable.EXTENT.toMetaRow());
    TabletColumnFamily.PREV_ROW_COLUMN.put(rootMutation, TabletColumnFamily.encodePrevEndRow(null));
    rootMutation.put(DataFileColumnFamily.NAME, new Text(rootRFile),
        new DataFileValue(1, 1).encodeAsValue());
    if (rootHasLogs) {
      rootMutation.put(LogColumnFamily.NAME, new Text("server/file:/fake-root-wal"),
          new Value("file:/fake-root-wal"));
    }
    rootMetadata.update(rootMutation);
    return new TestData(rootMetadata);
  }

  private void addTabletMetadata(SortedMap<Key,Value> entries, KeyExtent extent, String dataFile,
      boolean hasLogs) {
    Text row = extent.toMetaRow();
    entries.put(new Key(row, TabletColumnFamily.NAME, new Text(TabletColumnFamily.PREV_ROW_QUAL)),
        TabletColumnFamily.encodePrevEndRow(extent.prevEndRow()));
    if (dataFile != null) {
      StoredTabletFile tabletFile = new StoredTabletFile(dataFile);
      entries.put(new Key(row, DataFileColumnFamily.NAME, tabletFile.getMetaInsertText()),
          new DataFileValue(1, 1).encodeAsValue());
    }
    if (hasLogs) {
      entries.put(new Key(row, LogColumnFamily.NAME, new Text("server/file:/fake-wal")),
          new Value("file:/fake-wal"));
    }
  }

  private String createRFile(TableId tableId, String fileName, SortedMap<Key,Value> entries)
      throws IOException {
    java.nio.file.Path directory =
        tempDir.resolve("tables").resolve(tableId.canonical()).resolve("default_tablet");
    Files.createDirectories(directory);
    java.nio.file.Path file = directory.resolve(fileName);
    String fileUri = file.toUri().toString();
    try (RFileWriter writer = RFile.newWriter().to(fileUri).build()) {
      writer.append(entries.entrySet());
    }
    return fileUri;
  }

  private static class TestData {
    private final RootTabletMetadata rootMetadata;

    private TestData(RootTabletMetadata rootMetadata) {
      this.rootMetadata = rootMetadata;
    }
  }
}
