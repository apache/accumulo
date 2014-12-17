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
package org.apache.accumulo.tserver.log;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.tserver.log.DfsLogger.DFSLoggerInputStreams;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalWALRecoveryTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  LocalWALRecovery recovery;

  File walTarget;
  AccumuloConfiguration configuration;

  @Before
  public void setUp() throws Exception {
    File source = new File("src/test/resources", "walog-from-14");

    configuration = createMock(AccumuloConfiguration.class);
    expect(configuration.get(Property.LOGGER_DIR)).andReturn(source.getAbsolutePath()).anyTimes();
    replay(configuration);

    walTarget = folder.newFolder("wal");

    recovery = new LocalWALRecovery(configuration);
    recovery.parseArgs("--dfs-wal-directory", walTarget.getAbsolutePath());
  }

  @Test
  public void testRecoverLocalWriteAheadLogs() throws IOException {
    Path targetPath = new Path(walTarget.toURI());
    FileSystem fs = FileSystem.get(targetPath.toUri(), new Configuration());
    recovery.recoverLocalWriteAheadLogs(fs);

    FileStatus[] recovered = fs.listStatus(targetPath);
    assertEquals("Wrong number of WAL files recovered.", 1, recovered.length);

    final Path path = recovered[0].getPath();
    final VolumeManager volumeManager = VolumeManagerImpl.getLocal(folder.getRoot().getAbsolutePath());

    final DFSLoggerInputStreams streams = DfsLogger.readHeaderAndReturnStream(volumeManager, path, configuration);
    final DataInputStream input = streams.getDecryptingInputStream();

    final LogFileKey key = new LogFileKey();
    final LogFileValue value = new LogFileValue();
    int read = 0;

    while (true) {
      try {
        key.readFields(input);
        value.readFields(input);
        read++;
      } catch (EOFException ex) {
        break;
      }
    }

    assertEquals(104, read);
  }
}
