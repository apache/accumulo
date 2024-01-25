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

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.hadoop.conf.Configuration;
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

    String[] walArgs = {"--type", "wal", wal1};
    CreateEmpty.Opts walOpts = new CreateEmpty.Opts();
    walOpts.parseArgs("accumulo create-empty", walArgs);

    assertThrows(IllegalArgumentException.class,
        () -> createEmpty.createEmptyWal(walOpts, context));

    // create the file so it exists
    File f2 = new File(rf1);
    assertTrue(f2.createNewFile());

    String[] rfArgs = {"--type", "rfile", rf1};
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

    String[] args = {"--type", "rfile", file1, file2};
    CreateEmpty.Opts opts = new CreateEmpty.Opts();
    opts.parseArgs("accumulo create-empty", args);

    createEmpty.createEmptyRFile(opts, context);
    VolumeManager vm = context.getVolumeManager();
    assertTrue(vm.exists(new Path(file1)));
    assertTrue(vm.exists(new Path(file2)));
  }

  @Test
  public void createWalTest() throws Exception {
    CreateEmpty createEmpty = new CreateEmpty();

    String file1 = genFilename(tempDir.getAbsolutePath() + "/empty", ".wal");
    String file2 = genFilename(tempDir.getAbsolutePath() + "/empty", ".wal");

    String[] args = {"--type", "wal", file1, file2};
    CreateEmpty.Opts opts = new CreateEmpty.Opts();
    opts.parseArgs("accumulo create-empty", args);

    createEmpty.createEmptyWal(opts, context);

    checkWalContext(file1);
    checkWalContext(file2);
  }

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

  // tempDir is per test suite - generate a one-up count file for each call.
  private static final AtomicInteger fileCount = new AtomicInteger(0);

  private String genFilename(final String prefix, final String extension) {
    return prefix + fileCount.incrementAndGet() + extension;
  }
}
