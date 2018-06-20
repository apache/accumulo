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
import static org.apache.accumulo.core.security.crypto.CryptoEnvironment.Scope;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.crypto.impl.AESCryptoService;
import org.apache.accumulo.core.security.crypto.impl.NoCryptoService;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class CryptoTest {

  private static final int MARKER_INT = 0xCADEFEDD;
  private static final String MARKER_STRING = "1 2 3 a b c";
  public static final String CRYPTO_ON_CONF = "crypto-on-accumulo-site.xml";
  public static final String CRYPTO_OFF_CONF = "crypto-off-accumulo-site.xml";

  @Test
  public void testAESCryptoService() throws Exception {
    AESCryptoService cs = new AESCryptoService();
    byte[] resultingBytes = encrypt(cs, CRYPTO_ON_CONF);

    String stringifiedBytes = Arrays.toString(resultingBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(MARKER_STRING, MARKER_INT);

    assertNotEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(resultingBytes, AESCryptoService.AESCBCCryptoModule.VERSION, CRYPTO_ON_CONF);
  }

  @Test
  public void testNoEncryption() throws Exception {
    NoCryptoService cs = new NoCryptoService();
    byte[] encryptedBytes = encrypt(cs, CRYPTO_OFF_CONF);

    String stringifiedBytes = Arrays.toString(encryptedBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(MARKER_STRING, MARKER_INT);

    assertEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(encryptedBytes, NoCryptoService.VERSION, CRYPTO_OFF_CONF);
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

  private ArrayList<Key> testData() {
    ArrayList<Key> keys = new ArrayList<>();
    keys.add(new Key("a", "cf", "cq"));
    keys.add(new Key("a1", "cf", "cq"));
    keys.add(new Key("a2", "cf", "cq"));
    keys.add(new Key("a3", "cf", "cq"));
    return keys;
  }

  private <C extends CryptoService> byte[] encrypt(C cs, String configFile) throws Exception {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(configFile);
    CryptoService cryptoService = CryptoServiceFactory.getConfigured(conf);
    FileEncrypter encrypter = cryptoService.encryptFile(
        new CryptoEnvironment(Scope.WAL, conf.getAllPropertiesWithPrefix(Property.TABLE_PREFIX)));

    assertNotNull("CryptoService returned null FileEncrypter", encrypter);
    assertEquals(cryptoService.getClass(), cs.getClass());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    OutputStream encrypted = encrypter.encryptStream(new NoFlushOutputStream(dataOut));
    assertNotNull(encrypted);

    // dataOut.writeUTF(version);
    dataOut.writeUTF(MARKER_STRING);
    dataOut.writeInt(MARKER_INT);
    return out.toByteArray();
  }

  private void decrypt(byte[] resultingBytes, String version, String configFile) throws Exception {
    ByteArrayInputStream in = new ByteArrayInputStream(resultingBytes);
    DataInputStream dataIn = new DataInputStream(in);

    AccumuloConfiguration conf = setAndGetAccumuloConfig(configFile);
    CryptoService cryptoService = CryptoServiceFactory.getConfigured(conf);
    FileDecrypter decrypter = cryptoService.decryptFile(new CryptoEnvironment(Scope.WAL, version,
        conf.getAllPropertiesWithPrefix(Property.TABLE_PREFIX)));

    decrypter.decryptStream(dataIn);
    String markerString = dataIn.readUTF();
    int markerInt = dataIn.readInt();

    assertEquals(MARKER_STRING, markerString);
    assertEquals(MARKER_INT, markerInt);
    in.close();
    dataIn.close();
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
