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
package org.apache.accumulo.tserver.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.tserver.log.DfsLogger.LOG_FILE_HEADER_V4;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CreateEmptyTest {
  @TempDir
  private static File tempDir;

  private ServerContext context;

  @BeforeEach
  public void init() throws IOException {
    ConfigurationCopy config = new ConfigurationCopy(DefaultConfiguration.getInstance());
    config.set(Property.INSTANCE_VOLUMES.getKey(), "file:///");

    context = mock(ServerContext.class);
    expect(context.getCryptoFactory()).andReturn(new GenericCryptoServiceFactory()).anyTimes();
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    expect(context.getHadoopConf()).andReturn(new Configuration()).anyTimes();
    VolumeManager volumeManager = VolumeManagerImpl.get(config, new Configuration());
    expect(context.getVolumeManager()).andReturn(volumeManager).anyTimes();
    replay(context);
  }

  @AfterEach
  public void verifyMock() {
    verify(context);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @Test
  public void exceptionOnFileExistsTest() throws Exception {
    CreateEmpty createEmpty = new CreateEmpty();

    String wal1 = genFilename(tempDir.getAbsolutePath() + "/empty", ".wal");
    String rf1 = genFilename(tempDir.getAbsolutePath() + "/empty", ".rf");

    // create the file so it exists
    File f = new File(wal1);
    assertTrue(f.createNewFile());

    String[] walArgs = {"--type", "WAL", wal1};
    CreateEmpty.Opts walOpts = new CreateEmpty.Opts();
    walOpts.parseArgs("accumulo create-empty", walArgs);

    assertThrows(IllegalArgumentException.class,
        () -> createEmpty.createEmptyWal(walOpts, context));

    // create the file so it exists
    File f2 = new File(rf1);
    assertTrue(f2.createNewFile());

    String[] rfArgs = {"--type", "RF", rf1};
    CreateEmpty.Opts rfOpts = new CreateEmpty.Opts();
    rfOpts.parseArgs("accumulo create-empty", rfArgs);
    assertThrows(IllegalArgumentException.class,
        () -> createEmpty.createEmptyRFile(walOpts, context));
  }

  @Test
  public void createRfileTest() throws Exception {
    CreateEmpty createEmpty = new CreateEmpty();

    String file1 = genFilename(tempDir.getAbsolutePath() + "/empty", ".rf");
    String file2 = genFilename(tempDir.getAbsolutePath() + "/empty", ".rf");

    String[] args = {"--type", "RF", file1, file2};
    CreateEmpty.Opts opts = new CreateEmpty.Opts();
    opts.parseArgs("accumulo create-empty", args);

    createEmpty.createEmptyRFile(opts, context);
    VolumeManager vm = context.getVolumeManager();
    assertTrue(vm.exists(new Path(file1)));
    try (var scanner = RFile.newScanner().from(file1).build()) {
      assertEquals(0, scanner.stream().count());
    }

    assertTrue(vm.exists(new Path(file2)));
    try (var scanner = RFile.newScanner().from(file2).build()) {
      assertEquals(0, scanner.stream().count());
    }

  }

  /**
   * Validate that the default type is RF (RecoveryWithEmptyRFileIT also needs this(
   */
  @Test
  public void createRfileDefaultTest() throws Exception {
    CreateEmpty createEmpty = new CreateEmpty();

    String file1 = genFilename(tempDir.getAbsolutePath() + "/empty", ".rf");

    String[] args = {file1};
    CreateEmpty.Opts opts = new CreateEmpty.Opts();
    opts.parseArgs("accumulo create-empty", args);

    createEmpty.createEmptyRFile(opts, context);
    VolumeManager vm = context.getVolumeManager();
    assertTrue(vm.exists(new Path(file1)));
    try (var scanner = RFile.newScanner().from(file1).build()) {
      assertEquals(0, scanner.stream().count());
    }
  }

  @Test
  public void createWalTest() throws Exception {
    CreateEmpty createEmpty = new CreateEmpty();

    String file1 = genFilename(tempDir.getAbsolutePath() + "/empty", ".wal");
    String file2 = genFilename(tempDir.getAbsolutePath() + "/empty", ".wal");

    String[] args = {"--type", "WAL", file1, file2};
    CreateEmpty.Opts opts = new CreateEmpty.Opts();
    opts.parseArgs("accumulo create-empty", args);

    createEmpty.createEmptyWal(opts, context);

    checkWalContext(file1);
    readLogFile(file1);

    checkWalContext(file2);
  }

  /**
   * Reads the log file and looks for specific information (crypto id, event == OPEN)
   */
  private void checkWalContext(final String expected) throws IOException {
    Path path = new Path(expected);
    VolumeManager vm = context.getVolumeManager();
    assertTrue(vm.exists(path));

    vm.open(path);
    try (InputStream inputStream = vm.open(path).getWrappedStream();
        DataInputStream dis = new DataInputStream(inputStream)) {
      byte[] headerBuf = new byte[1024];
      int len = dis.read(headerBuf, 0, LOG_FILE_HEADER_V4.length());
      assertEquals(LOG_FILE_HEADER_V4.length(), len);
      assertEquals(LOG_FILE_HEADER_V4,
          new String(headerBuf, 0, LOG_FILE_HEADER_V4.length(), UTF_8));

      CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.WAL);
      CryptoService cryptoService = context.getCryptoFactory().getService(env,
          context.getConfiguration().getAllCryptoProperties());

      byte[] decryptionParams = cryptoService.getFileEncrypter(env).getDecryptionParameters();

      var cryptParams = CryptoUtils.readParams(dis);
      assertArrayEquals(decryptionParams, cryptParams);

      LogFileKey key = new LogFileKey();
      key.readFields(dis);

      assertEquals(key.event, LogEvents.OPEN);
      assertEquals("", key.tserverSession);
      assertNull(key.filename);
    }
  }

  /**
   * Scan through log file and check that there is one event.
   */
  private void readLogFile(final String filename) throws Exception {
    Path path = new Path(filename);
    LogFileKey key = new LogFileKey();
    LogFileValue value = new LogFileValue();

    FileSystem fs = context.getVolumeManager().getFileSystemByPath(path);

    CryptoEnvironment env = new CryptoEnvironmentImpl(CryptoEnvironment.Scope.WAL);
    CryptoService cryptoService = context.getCryptoFactory().getService(env,
        context.getConfiguration().getAllCryptoProperties());

    int eventCount = 0;
    try (final FSDataInputStream fsinput = fs.open(path);
        DataInputStream input = DfsLogger.getDecryptingStream(fsinput, cryptoService)) {
      while (true) {
        try {
          key.readFields(input);
          value.readFields(input);
        } catch (EOFException ex) {
          break;
        }
        eventCount++;
      }
    } catch (DfsLogger.LogHeaderIncompleteException e) {
      fail("Could not read header for {}" + path);
    } finally {
      // empty wal has 1 event (OPEN)
      assertEquals(1, eventCount);
    }
  }

  // tempDir is per test suite - generate a one-up count file for each call.
  private static final AtomicInteger fileCount = new AtomicInteger(0);

  private String genFilename(final String prefix, final String extension) {
    return prefix + fileCount.incrementAndGet() + extension;
  }
}
