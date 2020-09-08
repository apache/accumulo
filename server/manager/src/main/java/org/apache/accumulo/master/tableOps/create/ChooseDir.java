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
package org.apache.accumulo.master.tableOps.create;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.TableInfo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

class ChooseDir extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;

  ChooseDir(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    if (tableInfo.getInitialSplitSize() > 0) {
      createTableDirectoriesInfo(master);
    }
    return new PopulateMetadata(tableInfo);
  }

  @Override
  public void undo(long tid, Master master) throws Exception {
    VolumeManager fs = master.getVolumeManager();
    fs.deleteRecursively(new Path(tableInfo.getSplitDirsFile()));
  }

  /**
   * Create unique table directory names that will be associated with split values. Then write these
   * to the file system for later use during this FATE operation.
   */
  private void createTableDirectoriesInfo(Master master) throws IOException {
    SortedSet<Text> splits =
        Utils.getSortedSetFromFile(master.getInputStream(tableInfo.getSplitFile()), true);
    SortedSet<Text> tabletDirectoryInfo = createTabletDirectoriesSet(master, splits.size());
    writeTabletDirectoriesToFileSystem(master, tabletDirectoryInfo);
  }

  /**
   * Create a set of unique table directories. These will be associated with splits in a follow-on
   * FATE step.
   */
  private SortedSet<Text> createTabletDirectoriesSet(Master master, int num) {
    String tabletDir;
    UniqueNameAllocator namer = master.getContext().getUniqueNameAllocator();
    SortedSet<Text> splitDirs = new TreeSet<>();
    for (int i = 0; i < num; i++) {
      tabletDir = Constants.GENERATED_TABLET_DIRECTORY_PREFIX + namer.getNextName();
      splitDirs.add(new Text(tabletDir));
    }
    return splitDirs;
  }

  /**
   * Write the SortedSet of Tablet Directory names to the file system for use in the next phase of
   * the FATE operation.
   */
  private void writeTabletDirectoriesToFileSystem(Master master, SortedSet<Text> dirs)
      throws IOException {
    FileSystem fs = master.getVolumeManager().getDefaultVolume().getFileSystem();
    if (fs.exists(new Path(tableInfo.getSplitDirsFile())))
      fs.delete(new Path(tableInfo.getSplitDirsFile()), true);
    try (FSDataOutputStream stream = master.getOutputStream(tableInfo.getSplitDirsFile())) {
      for (Text dir : dirs)
        stream.writeBytes(dir + "\n");
    }
  }

}
