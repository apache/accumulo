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
package org.apache.accumulo.tserver;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeImpl;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.Test;

public class TabletServerSyncCheckTest {
  private static final String DFS_SUPPORT_APPEND = "dfs.support.append";

  @Test
  public void testFailureOnExplicitAppendFalseConf() {
    Configuration conf = new Configuration();
    conf.set(DFS_SUPPORT_APPEND, "false");

    FileSystem fs = new TestFileSystem(conf);
    assertThrows(RuntimeException.class,
        () -> new TestVolumeManagerImpl(Map.of("foo", new VolumeImpl(fs, "/"))));
  }

  private class TestFileSystem extends DistributedFileSystem {
    protected final Configuration conf;

    public TestFileSystem(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

  }

  private class TestVolumeManagerImpl extends VolumeManagerImpl {

    public TestVolumeManagerImpl(Map<String,Volume> volumes) {
      super(volumes, new ConfigurationCopy(Collections.emptyMap()), new Configuration());
    }

    @Override
    protected void ensureSyncIsEnabled() {
      super.ensureSyncIsEnabled();
    }

    @Override
    public void close() {

    }

    @Override
    public FSDataOutputStream create(Path dest) {
      return null;
    }

    @Override
    public FSDataOutputStream overwrite(Path path) {
      return null;
    }

    @Override
    public FSDataOutputStream create(Path path, boolean b, int int1, short int2, long long1) {
      return null;
    }

    @Override
    public boolean createNewFile(Path writable) {
      return false;
    }

    @Override
    public FSDataOutputStream createSyncable(Path logPath, int buffersize, short replication,
        long blockSize) {
      return null;
    }

    @Override
    public boolean delete(Path path) {
      return false;
    }

    @Override
    public boolean deleteRecursively(Path path) {
      return false;
    }

    @Override
    public boolean exists(Path path) {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) {
      return null;
    }

    @Override
    public FileSystem getFileSystemByPath(Path path) {
      return null;
    }

    @Override
    public Path matchingFileSystem(Path source, Set<String> options) {
      return null;
    }

    @Override
    public FileStatus[] listStatus(Path path) {
      return null;
    }

    @Override
    public boolean mkdirs(Path directory) {
      return false;
    }

    @Override
    public FSDataInputStream open(Path path) {
      return null;
    }

    @Override
    public boolean rename(Path path, Path newPath) {
      return false;
    }

    @Override
    public boolean moveToTrash(Path sourcePath) {
      return false;
    }

    @Override
    public short getDefaultReplication(Path logPath) {
      return 0;
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public FileStatus[] globStatus(Path path) {
      return null;
    }

    @Override
    public String choose(VolumeChooserEnvironment env, Set<String> options) {
      return null;
    }

  }
}
