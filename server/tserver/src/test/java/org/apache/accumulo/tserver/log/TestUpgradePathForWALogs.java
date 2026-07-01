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
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.server.log.SortedLogState.getFinishedMarkerPath;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.WithTestNames;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "PATH_TRAVERSAL_OUT"},
    justification = "paths not set by user input")
public class TestUpgradePathForWALogs extends WithTestNames {

  // older logs no longer compatible
  private static final String WALOG_FROM_15 = "org/apache/accumulo/tserver/walog-from-15.walog";
  // logs from versions 1.6 through 1.10 should be the same
  private static final String WALOG_FROM_16 = "org/apache/accumulo/tserver/walog-from-16.walog";
  // logs from 2.0 were changed for improved crypto
  private static final String WALOG_FROM_20 = "org/apache/accumulo/tserver/walog-from-20.walog";

  private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1);

  private ServerContext context;
  private TabletServer server;

  @TempDir
  private static java.nio.file.Path tempDir;

  private static java.nio.file.Path perTestTempSubDir;

  @BeforeEach
  public void setUp() throws Exception {
    context = createMock(ServerContext.class);
    server = createMock(TabletServer.class);

    // Create a new subdirectory for each test
    perTestTempSubDir = tempDir.resolve(testName());
    if (!Files.isDirectory(perTestTempSubDir)) {
      Files.createDirectories(perTestTempSubDir);
    }

    String path = perTestTempSubDir.toAbsolutePath().toString();

    VolumeManager fs = VolumeManagerImpl.getLocalForTesting(path);

    expect(server.getContext()).andReturn(context).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    expect(context.getCryptoFactory()).andReturn(new GenericCryptoServiceFactory()).anyTimes();
    expect(context.getVolumeManager()).andReturn(fs).anyTimes();
    expect(context.getScheduledExecutor()).andReturn(EXECUTOR).anyTimes();
    replay(server, context);
  }

  @AfterEach
  public void tearDown() {
    verify(server, context);
  }

  @AfterAll
  public static void shutdown() {
    EXECUTOR.shutdownNow();
  }

  /**
   * Since 2.0 this version of WAL is no longer compatible.
   */
  @Test
  public void testUpgradeOf15WALog() throws IOException {
    java.nio.file.Path testPath = perTestTempSubDir.toAbsolutePath();
    copyWalogToTestDir(WALOG_FROM_15);

    LogSorter logSorter = new LogSorter(server);
    LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

    assertThrows(IllegalArgumentException.class,
        () -> logProcessor.sort(context.getVolumeManager(), "/" + WALOG_FROM_15,
            new Path(testPath.toUri() + WALOG_FROM_15), testPath.toUri() + "manyMaps"));
  }

  @Test
  public void testBasic16WALogRead() throws IOException {
    String walogToTest = WALOG_FROM_16;
    java.nio.file.Path testPath = perTestTempSubDir.toAbsolutePath();
    String destPath = testPath.toUri() + "manyMaps";
    copyWalogToTestDir(walogToTest);

    assertFalse(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));

    LogSorter logSorter = new LogSorter(server);
    LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();

    logProcessor.sort(context.getVolumeManager(), "/" + walogToTest,
        new Path(testPath.toUri() + walogToTest), destPath);

    assertTrue(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));
  }

  @Test
  public void testBasic20WALogRead() throws IOException {
    String walogToTest = WALOG_FROM_20;
    java.nio.file.Path testPath = perTestTempSubDir.toAbsolutePath();
    String destPath = testPath.toUri() + "manyMaps";
    copyWalogToTestDir(walogToTest);

    assertFalse(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));

    LogSorter logSorter = new LogSorter(server);
    LogSorter.LogProcessor logProcessor = logSorter.new LogProcessor();
    logProcessor.sort(context.getVolumeManager(), "/" + walogToTest,
        new Path(testPath.toUri() + walogToTest), destPath);

    assertTrue(context.getVolumeManager().exists(getFinishedMarkerPath(destPath)));
  }

  private void copyWalogToTestDir(String walogToTest) throws IOException {
    var walogPath = perTestTempSubDir.resolve(walogToTest);
    Files.createDirectories(Objects.requireNonNull(walogPath.getParent()));

    try (
        InputStream walogStream =
            Objects.requireNonNull(getClass().getResourceAsStream("/" + walogToTest));
        OutputStream walogInHDFStream = Files.newOutputStream(walogPath)) {
      walogStream.transferTo(walogInHDFStream);
    }
  }

}
