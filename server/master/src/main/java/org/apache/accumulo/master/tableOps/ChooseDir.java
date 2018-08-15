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
package org.apache.accumulo.master.tableOps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ChooseDir extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(ChooseDir.class);

  private TableInfo tableInfo;

  ChooseDir(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // Constants.DEFAULT_TABLET_LOCATION has a leading slash prepended to it so we don't need to add
    // one here

    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(tableInfo.tableId);

    String baseDir = master.getFileSystem().choose(chooserEnv, ServerConstants.getBaseUris())
        + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableInfo.tableId;
    tableInfo.dir = baseDir + Constants.DEFAULT_TABLET_LOCATION;

    if (tableInfo.initialSplitSize > 0) {
      createTabletDirectoryFile(master, baseDir);
    }
    return new CreateDir(tableInfo);
  }

  @Override
  public void undo(long tid, Master master) throws Exception {
    VolumeManager fs = master.getFileSystem();
    fs.deleteRecursively(new Path(tableInfo.splitDirsFile));
  }

  private void createTabletDirectoryFile(Master master, String baseDir) throws IOException {
    SortedSet<Text> splits = Utils.getSortedSetFromFile(master.getInputStream(tableInfo
        .splitFile), true);

    log.info(">>>> Retrieved " + splits.size() + " from sorted set");
    for (Text s : splits) {
      ByteBuffer wrap = ByteBuffer.wrap(s.getBytes(), 0, s.getLength());
      byte[] bytes = ByteBufferUtil.toBytes(wrap);
      log.info(">>>> s: " + getBytesAsString(bytes, s.getLength()));
    }

    SortedSet<Text> tabletDirectoryInfo = createTabletDirectories(master.getFileSystem(),
        splits.size(), baseDir);
    writeSplitDirInfo(master, tabletDirectoryInfo);
  }

  private String getBytesAsString(byte[] split, int size) {
    StringBuilder sb = new StringBuilder();
    for (int ii = 0; ii < size; ii++) {
      String str = String.format("%02x", split[ii]);
      sb.append(str);
    }
    return sb.toString();
  }

  private SortedSet<Text> createTabletDirectories(VolumeManager fs, int num,
      String baseDir) {
    String tabletDir;

    UniqueNameAllocator namer = UniqueNameAllocator.getInstance();
    SortedSet<Text> splitDirs = new TreeSet<>();

    for (int i = 0; i < num; i++) {
      tabletDir = "/" + Constants.GENERATED_TABLET_DIRECTORY_PREFIX + namer.getNextName();
      splitDirs.add(new Text(baseDir + "/" + new Path(tabletDir).getName()));
    }
    return splitDirs;
  }

  private void writeSplitDirInfo(Master master, SortedSet<Text> dirs) throws IOException {
    FileSystem fs = master.getFileSystem().getDefaultVolume().getFileSystem();
    if (fs.exists(new Path(tableInfo.splitDirsFile)))
      fs.delete(new Path(tableInfo.splitDirsFile), true);
    try (FSDataOutputStream stream = master.getOutputStream(tableInfo.splitDirsFile)) {
      for (Text dir : dirs)
        stream.writeBytes(dir + "\n");
    }
  }

}
