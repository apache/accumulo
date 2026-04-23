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
package org.apache.accumulo.manager.tableOps.create;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.TableInfo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChooseDir extends AbstractFateOperation {
  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;
  private static final Logger log = LoggerFactory.getLogger(ChooseDir.class);

  ChooseDir(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(FateId fateId, FateEnv environment) {
    return 0;
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {
    if (tableInfo.getInitialSplitSize() > 0) {
      createTableDirectoriesInfo(env.getContext());
    }
    return new PopulateMetadata(tableInfo);
  }

  @Override
  public void undo(FateId fateId, FateEnv env) throws Exception {
    // Clean up split files if ChooseDir operation fails
    Path p = null;
    try {
      if (tableInfo.getInitialSplitSize() > 0) {
        p = tableInfo.getSplitDirsPath();
        FileSystem fs = p.getFileSystem(env.getContext().getHadoopConf());
        fs.delete(p, true);
      }
    } catch (IOException e) {
      log.error("Failed to undo ChooseDir operation and failed to clean up split files at {}", p,
          e);
    }
  }

  /**
   * Create unique table directory names that will be associated with split values. Then write these
   * to the file system for later use during this FATE operation.
   */
  private void createTableDirectoriesInfo(ServerContext ctx) throws IOException {
    SortedMap<Text,TabletMergeability> splits =
        Utils.getSortedSplitsFromFile(ctx, tableInfo.getSplitPath());
    SortedSet<Text> tabletDirectoryInfo = createTabletDirectoriesSet(ctx, splits.size());
    writeTabletDirectoriesToFileSystem(ctx, tabletDirectoryInfo);
  }

  /**
   * Create a set of unique table directories. These will be associated with splits in a follow-on
   * FATE step.
   */
  private static SortedSet<Text> createTabletDirectoriesSet(ServerContext ctx, int num) {
    String tabletDir;
    UniqueNameAllocator namer = ctx.getUniqueNameAllocator();
    SortedSet<Text> splitDirs = new TreeSet<>();
    Iterator<String> names = namer.getNextNames(num);
    for (int i = 0; i < num; i++) {
      tabletDir = Constants.GENERATED_TABLET_DIRECTORY_PREFIX + names.next();
      splitDirs.add(new Text(tabletDir));
    }
    return splitDirs;
  }

  /**
   * Write the SortedSet of Tablet Directory names to the file system for use in the next phase of
   * the FATE operation.
   */
  private void writeTabletDirectoriesToFileSystem(ServerContext ctx, SortedSet<Text> dirs)
      throws IOException {
    Path p = tableInfo.getSplitDirsPath();
    FileSystem fs = p.getFileSystem(ctx.getHadoopConf());
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    try (FSDataOutputStream stream = fs.create(p)) {
      for (Text dir : dirs) {
        stream.write((dir + "\n").getBytes(UTF_8));
      }
    }
  }

}
