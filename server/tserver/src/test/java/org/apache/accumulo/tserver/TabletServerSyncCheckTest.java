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
package org.apache.accumulo.tserver;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeImpl;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TabletServerSyncCheckTest {
  private static final String DFS_SUPPORT_APPEND = "dfs.support.append";

  @Test(expected = RuntimeException.class)
  public void testFailureOnExplicitAppendFalseConf() {
    Configuration conf = new Configuration();
    conf.set(DFS_SUPPORT_APPEND, "false");

    FileSystem fs = new TestFileSystem(conf);
    TestVolumeManagerImpl vm = new TestVolumeManagerImpl(ImmutableMap.<String,Volume> of("foo", new VolumeImpl(fs, "/")));

    vm.ensureSyncIsEnabled();
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
      super(volumes, volumes.values().iterator().next(), new ConfigurationCopy(Collections.<String,String> emptyMap()));
    }

    @Override
    protected void ensureSyncIsEnabled() {
      super.ensureSyncIsEnabled();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public FSDataOutputStream create(Path dest) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream create(Path path, boolean b) throws IOException {
      return null;
    }

    @Override
    public FSDataOutputStream create(Path path, boolean b, int int1, short int2, long long1) throws IOException {
      return null;
    }

    @Override
    public boolean createNewFile(Path writable) throws IOException {
      return false;
    }

    @Override
    public FSDataOutputStream createSyncable(Path logPath, int buffersize, short replication, long blockSize) throws IOException {
      return null;
    }

    @Override
    public boolean delete(Path path) throws IOException {
      return false;
    }

    @Override
    public boolean deleteRecursively(Path path) throws IOException {
      return false;
    }

    @Override
    public boolean exists(Path path) throws IOException {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      return null;
    }

    @Override
    public Volume getVolumeByPath(Path path) {
      return null;
    }

    @Override
    public Path matchingFileSystem(Path source, String[] options) {
      return null;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
      return null;
    }

    @Override
    public boolean mkdirs(Path directory) throws IOException {
      return false;
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
      return null;
    }

    @Override
    public boolean rename(Path path, Path newPath) throws IOException {
      return false;
    }

    @Override
    public boolean moveToTrash(Path sourcePath) throws IOException {
      return false;
    }

    @Override
    public short getDefaultReplication(Path logPath) {
      return 0;
    }

    @Override
    public boolean isFile(Path path) throws IOException {
      return false;
    }

    @Override
    public boolean isReady() throws IOException {
      return false;
    }

    @Override
    public FileStatus[] globStatus(Path path) throws IOException {
      return null;
    }

    @Override
    public Path getFullPath(Key key) {
      return null;
    }

    @Override
    public Path getFullPath(Table.ID tableId, String path) {
      return null;
    }

    @Override
    public Path getFullPath(FileType fileType, String fileName) {
      return null;
    }

    @Override
    public ContentSummary getContentSummary(Path dir) throws IOException {
      return null;
    }

    @Override
    public String choose(VolumeChooserEnvironment env, String[] options) {
      return null;
    }

  }
}
