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
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.server.log.SortedLogState.getFinishedMarkerPath;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "PATH_TRAVERSAL_OUT"},
    justification = "paths not set by user input")
public class TestUpgradePathForWALogs {

  // older logs no longer compatible
  private static final String WALOG_FROM_15 = "/walog-from-15.walog";
  // logs from versions 1.6 through 1.10 should be the same
  private static final String WALOG_FROM_16 = "/walog-from-16.walog";
  // logs from 2.0 were changed for improved crypto
  private static final String WALOG_FROM_20 = "/walog-from-20.walog";

  private static final AccumuloConfiguration config = DefaultConfiguration.getInstance();
  private ServerContext context;

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir"), "target"));

  @Before
  public void setUp() throws Exception {
    context = createMock(ServerContext.class);
    File workDir = tempFolder.newFolder();
    String path = workDir.getAbsolutePath();
    assertTrue(workDir.delete());
    VolumeManager fs = VolumeManagerImpl.getLocalForTesting(path);

    expect(context.getVolumeManager()).andReturn(fs).anyTimes();
    replay(context);
  }

  @After
  public void tearDown() {
    verify(context);
  }

  /**
   * Since 2.0 this version of WAL is no longer compatible.
   */
  @Test
  public void testUpgradeOf15WALog() throws IOException {
    String walogToTest = WALOG_FROM_15;
    String testPath = tempFolder.getRoot().getAbsolutePath();

    try (InputStream walogStream = getClass().getResourceAsStream(walogToTest);
        OutputStream walogInHDFStream = new FileOutputStream(testPath + walogToTest)) {
      IOUtils.copyLarge(walogStream, walogInHDFStream);
      walogInHDFStream.flush();
      walogInHDFStream.close();

      LogSorter logSorter = new LogSorter(context, config);
      LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

      assertThrows(IllegalArgumentException.class,
          () -> logProcessor.sort(context.getVolumeManager(), WALOG_FROM_15,
              new Path("file://" + testPath + WALOG_FROM_15), "file://" + testPath + "/manyMaps"));
    }
  }

  @Test
  public void testBasic16WALogRead() throws IOException {
    String walogToTest = WALOG_FROM_16;
    String testPath = tempFolder.getRoot().getAbsolutePath();
    String destPath = "file://" + testPath + "/manyMaps";

    try (InputStream walogStream = getClass().getResourceAsStream(walogToTest);
        OutputStream walogInHDFStream = new FileOutputStream(testPath + walogToTest)) {
      IOUtils.copyLarge(walogStream, walogInHDFStream);
      walogInHDFStream.flush();
      walogInHDFStream.close();

      assertFalse(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));

      LogSorter logSorter = new LogSorter(context, config);
      LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

      logProcessor.sort(context.getVolumeManager(), walogToTest,
          new Path("file://" + testPath + walogToTest), destPath);

      assertTrue(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));
    }
  }

  @Test
  public void testBasic20WALogRead() throws IOException {
    String walogToTest = WALOG_FROM_20;
    String testPath = tempFolder.getRoot().getAbsolutePath();
    String destPath = "file://" + testPath + "/manyMaps";

    try (InputStream walogStream = getClass().getResourceAsStream(walogToTest);
        OutputStream walogInHDFStream = new FileOutputStream(testPath + walogToTest)) {
      IOUtils.copyLarge(walogStream, walogInHDFStream);
      walogInHDFStream.flush();
      walogInHDFStream.close();

      assertFalse(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));

      LogSorter logSorter = new LogSorter(context, config);
      LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();
      logProcessor.sort(context.getVolumeManager(), walogToTest,
          new Path("file://" + testPath + walogToTest), destPath);

      assertTrue(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));
    }
  }

}
