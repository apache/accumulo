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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// base class for ITs that test our legacy trash property and Hadoop's trash policy with accumulo gc
public class GarbageCollectorTrashBase extends ConfigurableMacBase {

  private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorTrashBase.class);

  protected ArrayList<StoredTabletFile> getFilesForTable(ServerContext ctx, AccumuloClient client,
      String tableName) {
    String tid = client.tableOperations().tableIdMap().get(tableName);
    try (TabletsMetadata tms =
        ctx.getAmple().readTablets().forTable(TableId.of(tid)).fetch(ColumnType.FILES).build()) {
      ArrayList<StoredTabletFile> files =
          tms.stream().flatMap(tabletMetadata -> tabletMetadata.getFiles().stream())
              .collect(Collectors.toCollection(ArrayList::new));
      LOG.debug("Tablet files: {}", files);
      return files;
    }
  }

  protected ArrayList<StoredTabletFile> loadData(ServerContext ctx, AccumuloClient client,
      String tableName) throws Exception {
    // create some files
    for (int i = 0; i < 5; i++) {
      ReadWriteIT.ingest(client, 10, 10, 10, 0, tableName);
      client.tableOperations().flush(tableName, null, null, true);
    }
    return getFilesForTable(ctx, client, tableName);
  }

  protected boolean userTrashDirExists(FileSystem fs) {
    return !fs.getTrashRoots(false).isEmpty();
  }

  protected void makeTrashDir(FileSystem fs) throws IOException {
    if (!userTrashDirExists(fs)) {
      Path homeDir = fs.getHomeDirectory();
      Path trashDir = new Path(homeDir, ".Trash");
      assertTrue(fs.mkdirs(trashDir));
    }
    assertTrue(userTrashDirExists(fs));

  }

  protected void waitForFilesToBeGCd(final ArrayList<StoredTabletFile> files) throws Exception {
    Wait.waitFor(() -> files.stream().noneMatch(stf -> {
      try {
        return super.getCluster().getMiniDfs().getFileSystem().exists(stf.getPath());
      } catch (IOException e) {
        throw new UncheckedIOException("error", e);
      }
    }));
  }

  protected long countFilesInTrash(FileSystem fs, TableId tid)
      throws FileNotFoundException, IOException {
    Collection<FileStatus> dirs = fs.getTrashRoots(true);
    if (dirs.isEmpty()) {
      return -1;
    }
    long count = 0;
    Iterator<FileStatus> iter = dirs.iterator();
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      LOG.debug("Trash root: {}", stat.getPath());
      RemoteIterator<LocatedFileStatus> riter = fs.listFiles(stat.getPath(), true);
      while (riter.hasNext()) {
        LocatedFileStatus lfs = riter.next();
        if (lfs.isDirectory()) {
          continue;
        }
        ReferencedTabletFile tf = new ReferencedTabletFile(lfs.getPath());
        LOG.debug("File in trash: {}, tableId: {}", lfs.getPath(), tf.getTableId());
        if (tid.equals(tf.getTableId())) {
          count++;
        }
      }
    }
    return count;
  }

}
