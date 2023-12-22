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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.tserver.log.DfsLogger.LOG_FILE_HEADER_V4;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.spi.crypto.NoFileEncrypter;
import org.apache.accumulo.tserver.logger.LogEvents;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.beust.jcommander.ParameterException;

public class CreateEmptyWalTest {

  @TempDir
  private static File tempDir;

  @Test
  public void createTest() throws Exception {
    String[] args = {"-d", tempDir.getAbsolutePath()};
    CreateEmptyWal uut = new CreateEmptyWal();
    uut.execute(args);
    Path expected = Path.of(tempDir.getAbsolutePath() + "/empty.wal");
    assertTrue(Files.exists(expected));

    try (FileInputStream fis = new FileInputStream(expected.toFile());
        DataInputStream dis = new DataInputStream(fis)) {
      byte[] headerBuf = new byte[1024];
      int len = dis.read(headerBuf, 0, LOG_FILE_HEADER_V4.length());
      assertEquals(LOG_FILE_HEADER_V4.length(), len);
      assertEquals(LOG_FILE_HEADER_V4,
          new String(headerBuf, 0, LOG_FILE_HEADER_V4.length(), UTF_8));

      byte[] decryptionParams = new NoFileEncrypter().getDecryptionParameters();

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
   * test the validator directly, during normal execution, command parsing will print help and then
   * exit with an error code. This causes issues trying to test with maven when the VM exits.
   */
  @Test
  public void noDIrFailsTest() {
    var validator = new CreateEmptyWal.DirValidator();
    assertThrows(ParameterException.class, () -> validator.validate("-d", "a/b/c/afakedirectory"));
  }

}
