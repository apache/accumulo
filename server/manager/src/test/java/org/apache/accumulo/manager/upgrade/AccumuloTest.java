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
package org.apache.accumulo.manager.upgrade;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class AccumuloTest {
  private ServerContext context;
  private FileSystem fs;
  private Path path;
  private ServerDirs serverDirs;

  @BeforeEach
  public void setUp() {
    context = createMock(ServerContext.class);
    fs = createMock(FileSystem.class);
    path = createMock(Path.class);
    replay(context, fs, path);
    serverDirs = new ServerDirs(DefaultConfiguration.getInstance(), new Configuration());
  }

  @AfterEach
  public void verifyMocks() {
    verify(context, fs, path);
  }

  private FileStatus[] mockPersistentVersion(String s) {
    var fileStatus = new FileStatus();
    fileStatus.setPath(new Path(s));
    return new FileStatus[] {fileStatus};
  }

  @Test
  public void testGetAccumuloPersistentVersion() throws Exception {
    FileStatus[] files = mockPersistentVersion("42");
    reset(fs);
    expect(fs.listStatus(path)).andReturn(files);
    replay(fs);

    assertEquals(42, serverDirs.getAccumuloPersistentVersion(fs, path));
  }

  @Test
  public void testGetAccumuloPersistentVersion_Null() throws Exception {
    reset(fs);
    expect(fs.listStatus(path)).andReturn(null);
    replay(fs);

    assertEquals(-1, serverDirs.getAccumuloPersistentVersion(fs, path));
  }

  @Test
  public void testGetAccumuloPersistentVersion_Empty() throws Exception {
    reset(fs);
    expect(fs.listStatus(path)).andReturn(new FileStatus[0]);
    replay(fs);

    assertEquals(-1, serverDirs.getAccumuloPersistentVersion(fs, path));
  }

  @Test
  public void testGetAccumuloPersistentVersion_Fail() throws Exception {
    reset(fs);
    expect(fs.listStatus(path)).andThrow(new FileNotFoundException());
    replay(fs);
    assertThrows(RuntimeException.class, () -> serverDirs.getAccumuloPersistentVersion(fs, path));
  }

  @Test
  public void testUpdateAccumuloVersion() throws Exception {
    Volume v1 = createMock(Volume.class);
    FileSystem fs1 = createMock(FileSystem.class);
    Path baseVersion1 = new Path("hdfs://volume1/accumulo/version");
    Path oldVersion1 = new Path("hdfs://volume1/accumulo/version/7");
    Path newVersion1 = new Path("hdfs://volume1/accumulo/version/" + AccumuloDataVersion.get());

    FileStatus[] files1 = mockPersistentVersion("7");
    expect(fs1.listStatus(baseVersion1)).andReturn(files1);
    replay(fs1);

    FSDataOutputStream fsdos1 = createMock(FSDataOutputStream.class);
    expect(v1.getFileSystem()).andReturn(fs1);
    expect(v1.prefixChild(Constants.VERSION_DIR)).andReturn(baseVersion1).times(2);
    replay(v1);
    fsdos1.close();
    replay(fsdos1);

    Volume v2 = createMock(Volume.class);
    FileSystem fs2 = createMock(FileSystem.class);
    Path baseVersion2 = new Path("hdfs://volume2/accumulo/version");
    Path oldVersion2 = new Path("hdfs://volume2/accumulo/version/7");
    Path newVersion2 = new Path("hdfs://volume2/accumulo/version/" + AccumuloDataVersion.get());

    FileStatus[] files2 = mockPersistentVersion("7");
    expect(fs2.listStatus(baseVersion2)).andReturn(files2);
    replay(fs2);

    FSDataOutputStream fsdos2 = createMock(FSDataOutputStream.class);
    expect(v2.getFileSystem()).andReturn(fs2);
    expect(v2.prefixChild(Constants.VERSION_DIR)).andReturn(baseVersion2).times(2);
    replay(v2);
    fsdos2.close();
    replay(fsdos2);

    VolumeManager vm = createMock(VolumeManager.class);
    expect(vm.getVolumes()).andReturn(Sets.newHashSet(v1, v2));
    expect(vm.delete(oldVersion1)).andReturn(true);
    expect(vm.create(newVersion1)).andReturn(fsdos1);
    expect(vm.delete(oldVersion2)).andReturn(true);
    expect(vm.create(newVersion2)).andReturn(fsdos2);
    replay(vm);

    UpgradeCoordinator upgradeCoordinator = new UpgradeCoordinator(context);
    ServerDirs constants = new ServerDirs(DefaultConfiguration.getInstance(), new Configuration());
    upgradeCoordinator.updateAccumuloVersion(constants, vm, 7);

    verify(v1, fs1, fsdos1, v2, fs2, fsdos2, vm);
  }
}
