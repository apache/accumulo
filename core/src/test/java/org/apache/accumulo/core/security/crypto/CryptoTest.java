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

package org.apache.accumulo.core.security.crypto;

import static org.apache.accumulo.core.file.rfile.RFileTest.setAndGetAccumuloConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CryptoTest {

  private static final int MARKER_INT = 0xCADEFEDD;
  private static final String MARKER_STRING = "1 2 3 a b c";
  public static final String CRYPTO_ON_CONF = "crypto-on-accumulo-site.xml";
  public static final String CRYPTO_OFF_CONF = "crypto-off-accumulo-site.xml";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testAESEncryptionStrategy() throws Exception {
    byte[] resultingBytes = encrypt(AESCBCEncryptionStrategy.class, CRYPTO_ON_CONF);

    String stringifiedBytes = Arrays.toString(resultingBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(MARKER_STRING, MARKER_INT);

    assertNotEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(resultingBytes, CRYPTO_ON_CONF);
  }

  @Test
  public void testNoEncryption() throws Exception {
    byte[] resultingBytes = encrypt(NoEncryptionStrategy.class, CRYPTO_OFF_CONF);

    String stringifiedBytes = Arrays.toString(resultingBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(MARKER_STRING, MARKER_INT);

    assertEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(resultingBytes, CRYPTO_OFF_CONF);
  }

  @Test
  public void testRFileEncrypted() throws Exception {
    AccumuloConfiguration cryptoOnConf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);
    FileSystem fs = FileSystem.getLocal(CachedConfiguration.getInstance());
    ArrayList<Key> keys = testData();

    String file = "target/testFile.rf";
    fs.delete(new Path(file), true);
    try (RFileWriter writer = RFile.newWriter().to(file).withFileSystem(fs)
        .withTableProperties(cryptoOnConf).build()) {
      Value empty = new Value(new byte[] {});
      writer.startDefaultLocalityGroup();
      for (Key key : keys) {
        writer.append(key, empty);
      }
    }

    Scanner iter = RFile.newScanner().from(file).withFileSystem(fs)
        .withTableProperties(cryptoOnConf).build();
    ArrayList<Key> keysRead = new ArrayList<>();
    iter.forEach(e -> keysRead.add(e.getKey()));
    assertEquals(keys, keysRead);
  }

  @Test
  public void testRFileEncryptedDiffConf() throws Exception {
    String exceptionMsg = "File encrypted with different encryption";
    AccumuloConfiguration cryptoOnConf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);
    AccumuloConfiguration cryptoOffConf = setAndGetAccumuloConfig(CRYPTO_OFF_CONF);
    FileSystem fs = FileSystem.getLocal(CachedConfiguration.getInstance());
    ArrayList<Key> keys = testData();

    String file = "target/testFile.rf";
    fs.delete(new Path(file), true);
    try (RFileWriter writer = RFile.newWriter().to(file).withFileSystem(fs)
        .withTableProperties(cryptoOnConf).build()) {
      Value empty = new Value(new byte[] {});
      writer.startDefaultLocalityGroup();
      for (Key key : keys) {
        writer.append(key, empty);
      }
    }

    Scanner iter = RFile.newScanner().from(file).withFileSystem(fs)
        .withTableProperties(cryptoOffConf).build();
    ArrayList<Key> keysRead = new ArrayList<>();
    try {
      iter.forEach(e -> keysRead.add(e.getKey()));
      assertTrue("Scanner should have thrown exception " + exceptionMsg, false);
    } catch (RuntimeException re) {
      if (!re.getMessage().startsWith(exceptionMsg))
        throw re;
    }
  }

  private ArrayList<Key> testData() {
    ArrayList<Key> keys = new ArrayList<>();
    keys.add(new Key("a", "cf", "cq"));
    keys.add(new Key("a1", "cf", "cq"));
    keys.add(new Key("a2", "cf", "cq"));
    keys.add(new Key("a3", "cf", "cq"));
    return keys;
  }

  private byte[] encrypt(Class strategyClass, String configFile) throws Exception {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(configFile);
    EncryptionStrategy strategy = EncryptionStrategyFactory.setupConfiguredEncryption(conf,
        EncryptionStrategy.Scope.RFILE);

    assertEquals(strategyClass, strategy.getClass());
    // test init on other scope to be sure, even though this test has no scope
    assertTrue(strategy.init(EncryptionStrategy.Scope.WAL, conf));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    OutputStream encrypted = strategy.encryptStream(new NoFlushOutputStream(out));
    assertNotNull(encrypted);

    DataOutputStream dataOut = new DataOutputStream(encrypted);
    dataOut.writeUTF(MARKER_STRING);
    dataOut.writeInt(MARKER_INT);
    dataOut.close();
    return out.toByteArray();
  }

  private void decrypt(byte[] resultingBytes, String configFile) throws Exception {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(configFile);
    EncryptionStrategy strategy = EncryptionStrategyFactory.setupConfiguredEncryption(conf,
        EncryptionStrategy.Scope.RFILE);

    // test init on other scope to be sure, even though this test has no scope
    assertTrue(strategy.init(EncryptionStrategy.Scope.WAL, conf));

    ByteArrayInputStream in = new ByteArrayInputStream(resultingBytes);
    DataInputStream decrypted = new DataInputStream(strategy.decryptStream(in));
    String markerString = decrypted.readUTF();
    int markerInt = decrypted.readInt();

    assertEquals(MARKER_STRING, markerString);
    assertEquals(MARKER_INT, markerInt);
  }

  private String getStringifiedBytes(String s, int i) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);

    dataOut.writeUTF(s);
    dataOut.writeInt(i);
    dataOut.close();
    byte[] stringMarkerBytes = out.toByteArray();
    return Arrays.toString(stringMarkerBytes);
  }

}
