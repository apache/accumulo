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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map.Entry;

import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.primitives.Bytes;

public class CryptoTest {

  private static final int MARKER_INT = 0xCADEFEDD;
  private static final String MARKER_STRING = "1 2 3 a b c";
  public static final String CRYPTO_ON_CONF = "crypto-on-accumulo-site.xml";
  public static final String CRYPTO_OFF_CONF = "crypto-off-accumulo-site.xml";
  public static final String CRYPTO_ON_KEK_OFF_CONF = "crypto-on-no-key-encryption-accumulo-site.xml";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testNoCryptoStream() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_OFF_CONF);

    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);

    assertNotNull(params);
    assertEquals("NullCipher", params.getCipherSuite());

    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);
    assertNotNull(cryptoModule);
    assertTrue(cryptoModule instanceof CryptoModuleFactory.NullCryptoModule);

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    params.setPlaintextOutputStream(out);

    params = cryptoModule.getEncryptingOutputStream(params);
    assertNotNull(params.getEncryptedOutputStream());
    assertEquals(out, params.getEncryptedOutputStream());
  }

  @Test
  public void testCryptoModuleParamsParsing() {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);

    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);

    assertNotNull(params);
    assertEquals("AES/GCM/NoPadding", params.getCipherSuite());
    assertEquals("AES/CBC/NoPadding", params.getAllOptions().get(Property.CRYPTO_WAL_CIPHER_SUITE.getKey()));
    assertEquals("GCM", params.getCipherSuiteEncryptionMode());
    assertEquals("AES", params.getKeyAlgorithmName());
    assertEquals(128, params.getKeyLength());
    assertEquals("SHA1PRNG", params.getRandomNumberGenerator());
    assertEquals("SUN", params.getRandomNumberGeneratorProvider());
    assertEquals("SunJCE", params.getSecurityProvider());
    assertEquals("org.apache.accumulo.core.security.crypto.CachingHDFSSecretKeyEncryptionStrategy", params.getKeyEncryptionStrategyClass());
  }

  @Test
  public void testCryptoModuleDoesntLeakSensitive() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);

    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    params.setPlaintextOutputStream(baos);

    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);

    cryptoModule.getEncryptingOutputStream(params);
    params.getEncryptedOutputStream().close();

    // If we get here, we have encrypted bytes
    byte[] streamBytes = baos.toByteArray();
    for (Property prop : Property.values()) {
      if (prop.isSensitive()) {
        byte[] toCheck = prop.getKey().getBytes();
        assertEquals(-1, Bytes.indexOf(streamBytes, toCheck));
      }
    }

  }

  @Test
  public void testCryptoModuleParamsValidation1() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);

    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);

    assertTrue(cryptoModule instanceof DefaultCryptoModule);

    exception.expect(RuntimeException.class);
    cryptoModule.getEncryptingOutputStream(params);
  }

  @Test
  public void testCryptoModuleParamsValidation2() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);

    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);

    assertTrue(cryptoModule instanceof DefaultCryptoModule);

    exception.expect(RuntimeException.class);
    cryptoModule.getDecryptingInputStream(params);
  }

  private String getStringifiedBytes(String s) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);

    dataOut.writeUTF(s);
    dataOut.close();
    byte[] stringMarkerBytes = out.toByteArray();
    return Arrays.toString(stringMarkerBytes);

  }

  private String getStringifiedBytes(int i) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);

    dataOut.writeInt(i);
    dataOut.close();
    byte[] stringMarkerBytes = out.toByteArray();
    return Arrays.toString(stringMarkerBytes);

  }

  @Test
  public void testCryptoModuleBasicReadWrite() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_KEK_OFF_CONF);

    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);
    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);

    assertTrue(cryptoModule instanceof DefaultCryptoModule);

    byte[] resultingBytes = setUpSampleEncryptedBytes(cryptoModule, params);

    // If we get here, we have encrypted bytes
    ByteArrayInputStream in = new ByteArrayInputStream(resultingBytes);

    params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    params.setEncryptedInputStream(in);

    params = cryptoModule.getDecryptingInputStream(params);

    InputStream plaintextIn = params.getPlaintextInputStream();

    assertNotNull(plaintextIn);
    assertNotSame(plaintextIn, in);
    DataInputStream dataIn = new DataInputStream(plaintextIn);
    String markerString = dataIn.readUTF();
    int markerInt = dataIn.readInt();

    assertEquals(MARKER_STRING, markerString);
    assertEquals(MARKER_INT, markerInt);
  }

  private byte[] setUpSampleEncryptedBytes(CryptoModule cryptoModule, CryptoModuleParameters params) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    params.setPlaintextOutputStream(new NoFlushOutputStream(out));

    params = cryptoModule.getEncryptingOutputStream(params);

    assertNotNull(params.getEncryptedOutputStream());
    assertNotSame(params.getEncryptedOutputStream(), out);

    DataOutputStream dataOut = new DataOutputStream(params.getEncryptedOutputStream());
    dataOut.writeUTF(MARKER_STRING);
    dataOut.writeInt(MARKER_INT);
    dataOut.close();

    byte[] resultingBytes = out.toByteArray();
    String stringifiedBytes = Arrays.toString(resultingBytes);

    String stringifiedMarkerBytes = getStringifiedBytes(MARKER_STRING);
    String stringifiedOtherBytes = getStringifiedBytes(MARKER_INT);

    // OK, let's make sure it's encrypted
    assertFalse(stringifiedBytes.contains(stringifiedMarkerBytes));
    assertFalse(stringifiedBytes.contains(stringifiedOtherBytes));
    return resultingBytes;
  }

  @Test
  public void testKeyEncryptionAndCheckThatFileCannotBeReadWithoutKEK() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);

    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);
    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);

    // CRYPTO_ON_CONF uses AESWrap which produces wrapped keys that are too large and require a change to
    // JCE Unlimited Strength Jurisdiction. Using AES/ECB/NoPadding should avoid this problem.
    params.getAllOptions().put(Property.CRYPTO_DEFAULT_KEY_STRATEGY_CIPHER_SUITE.getKey(), "AES/ECB/NoPadding");
    assertTrue(cryptoModule instanceof DefaultCryptoModule);
    assertNotNull(params.getKeyEncryptionStrategyClass());
    assertEquals("org.apache.accumulo.core.security.crypto.CachingHDFSSecretKeyEncryptionStrategy", params.getKeyEncryptionStrategyClass());

    byte[] resultingBytes = setUpSampleEncryptedBytes(cryptoModule, params);

    // So now that we have bytes encrypted by a key encrypted to a KEK, turn off the KEK configuration and try
    // to decrypt. We expect this to fail. This also tests our ability to override the key encryption strategy.
    conf = setAndGetAccumuloConfig(CRYPTO_ON_KEK_OFF_CONF);
    params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    params.setOverrideStreamsSecretKeyEncryptionStrategy(true);

    ByteArrayInputStream in = new ByteArrayInputStream(resultingBytes);
    params.setEncryptedInputStream(in);

    params = cryptoModule.getDecryptingInputStream(params);

    assertNotNull(params.getPlaintextInputStream());
    DataInputStream dataIn = new DataInputStream(params.getPlaintextInputStream());
    // We expect the following operation to fail and throw an exception
    exception.expect(IOException.class);
    @SuppressWarnings("unused")
    String markerString = dataIn.readUTF();
  }

  @Test
  public void testKeyEncryptionNormalPath() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);

    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);
    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);

    assertTrue(cryptoModule instanceof DefaultCryptoModule);
    assertNotNull(params.getKeyEncryptionStrategyClass());
    assertEquals("org.apache.accumulo.core.security.crypto.CachingHDFSSecretKeyEncryptionStrategy", params.getKeyEncryptionStrategyClass());

    byte[] resultingBytes = setUpSampleEncryptedBytes(cryptoModule, params);

    params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    params.setOverrideStreamsSecretKeyEncryptionStrategy(true);

    ByteArrayInputStream in = new ByteArrayInputStream(resultingBytes);
    params.setEncryptedInputStream(in);

    params = cryptoModule.getDecryptingInputStream(params);

    assertNotNull(params.getPlaintextInputStream());
    DataInputStream dataIn = new DataInputStream(params.getPlaintextInputStream());

    String markerString = dataIn.readUTF();
    int markerInt = dataIn.readInt();

    assertEquals(MARKER_STRING, markerString);
    assertEquals(MARKER_INT, markerInt);
  }

  @Test
  public void testChangingCryptoParamsAndCanStillDecryptPreviouslyEncryptedFiles() throws IOException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);

    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);
    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);

    assertTrue(cryptoModule instanceof DefaultCryptoModule);
    assertNotNull(params.getKeyEncryptionStrategyClass());
    assertEquals("org.apache.accumulo.core.security.crypto.CachingHDFSSecretKeyEncryptionStrategy", params.getKeyEncryptionStrategyClass());

    byte[] resultingBytes = setUpSampleEncryptedBytes(cryptoModule, params);

    // Now we're going to create a params object and set its algorithm and key length different
    // from those configured within the site configuration. After doing this, we should
    // still be able to read the file that was created with a different set of parameters.
    params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    params.setKeyAlgorithmName("DESede");
    params.setKeyLength(24 * 8);

    ByteArrayInputStream in = new ByteArrayInputStream(resultingBytes);
    params.setEncryptedInputStream(in);

    params = cryptoModule.getDecryptingInputStream(params);

    assertNotNull(params.getPlaintextInputStream());
    DataInputStream dataIn = new DataInputStream(params.getPlaintextInputStream());
    String markerString = dataIn.readUTF();
    int markerInt = dataIn.readInt();

    assertEquals(MARKER_STRING, markerString);
    assertEquals(MARKER_INT, markerInt);
  }

  private AccumuloConfiguration setAndGetAccumuloConfig(String cryptoConfSetting) {
    ConfigurationCopy result = new ConfigurationCopy(DefaultConfiguration.getInstance());
    Configuration conf = new Configuration(false);
    conf.addResource(cryptoConfSetting);
    for (Entry<String,String> e : conf) {
      result.set(e.getKey(), e.getValue());
    }
    return result;
  }

  @Test
  public void testKeyWrapAndUnwrap() throws NoSuchAlgorithmException, NoSuchPaddingException, NoSuchProviderException, InvalidKeyException,
      IllegalBlockSizeException, BadPaddingException {
    Cipher keyWrapCipher = Cipher.getInstance("AESWrap/ECB/NoPadding");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");

    byte[] kek = new byte[16];
    random.nextBytes(kek);
    byte[] randomKey = new byte[16];
    random.nextBytes(randomKey);

    keyWrapCipher.init(Cipher.WRAP_MODE, new SecretKeySpec(kek, "AES"));

    Key randKey = new SecretKeySpec(randomKey, "AES");

    byte[] wrappedKey = keyWrapCipher.wrap(randKey);

    assertNotNull(wrappedKey);
    // AESWrap will produce 24 bytes given 128 bits of key data with a 128-bit KEK
    assertEquals(wrappedKey.length, randomKey.length + 8);

    Cipher keyUnwrapCipher = Cipher.getInstance("AESWrap/ECB/NoPadding");
    keyUnwrapCipher.init(Cipher.UNWRAP_MODE, new SecretKeySpec(kek, "AES"));
    Key unwrappedKey = keyUnwrapCipher.unwrap(wrappedKey, "AES", Cipher.SECRET_KEY);

    byte[] unwrappedKeyBytes = unwrappedKey.getEncoded();
    assertArrayEquals(unwrappedKeyBytes, randomKey);

  }

  @Test
  public void AESGCM_Encryption_Test_Correct_Encryption_And_Decryption() throws IOException, AEADBadTagException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);
    byte[] encryptedBytes = testEncryption(conf, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20});
    Integer result = testDecryption(conf, encryptedBytes);
    assertEquals(result, Integer.valueOf(1));
  }

  @Test
  public void AESGCM_Encryption_Test_Tag_Integrity_Compromised() throws IOException, AEADBadTagException {
    AccumuloConfiguration conf = setAndGetAccumuloConfig(CRYPTO_ON_CONF);
    byte[] encryptedBytes = testEncryption(conf, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20});

    encryptedBytes[encryptedBytes.length - 1]++; // modify the tag
    exception.expect(AEADBadTagException.class);
    testDecryption(conf, encryptedBytes);
    encryptedBytes[encryptedBytes.length - 1]--;
    encryptedBytes[1486]++; // modify the data
    exception.expect(AEADBadTagException.class);
    testDecryption(conf, encryptedBytes);
  }

  @Test
  public void testIVIncrements() {
    // One byte
    byte[] testIv1 = new byte[1];
    // 11111110
    testIv1[0] = (byte) 0xFE;

    // 11111111
    CryptoModuleParameters.incrementIV(testIv1, testIv1.length - 1);
    assertArrayEquals(testIv1, new byte[] {(byte) 0xff});

    // 00000000
    CryptoModuleParameters.incrementIV(testIv1, testIv1.length - 1);
    assertArrayEquals(testIv1, new byte[] {(byte) 0x00});

    // Two bytes
    byte[] testIv2 = new byte[2];
    // 00000000 11111110
    testIv2[0] = (byte) 0x00;
    testIv2[1] = (byte) 0xFE;

    // 00000000 11111111
    CryptoModuleParameters.incrementIV(testIv2, testIv2.length - 1);
    assertArrayEquals(testIv2, new byte[] {(byte) 0x00, (byte) 0xFF});

    // 00000001 00000000
    CryptoModuleParameters.incrementIV(testIv2, testIv2.length - 1);
    assertArrayEquals(testIv2, new byte[] {(byte) 0x01, (byte) 0x00});

    // 00000001 00000001
    CryptoModuleParameters.incrementIV(testIv2, testIv2.length - 1);
    assertArrayEquals(testIv2, new byte[] {(byte) 0x01, (byte) 0x01});

    // 11111111 11111111
    testIv2[0] = (byte) 0xFF;
    testIv2[1] = (byte) 0xFF;

    // 00000000 00000000
    CryptoModuleParameters.incrementIV(testIv2, testIv2.length - 1);
    assertArrayEquals(testIv2, new byte[] {(byte) 0x00, (byte) 0x00});

    // Three bytes
    byte[] testIv3 = new byte[3];
    // 00000000 00000000 11111110
    testIv3[0] = (byte) 0x00;
    testIv3[1] = (byte) 0x00;
    testIv3[2] = (byte) 0xFE;

    // 00000000 00000000 11111111
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0xFF});

    // 00000000 00000001 00000000
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x00, (byte) 0x01, (byte) 0x00});

    // 00000000 00000001 00000001
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x00, (byte) 0x01, (byte) 0x01});

    // 00000000 11111111 11111110
    testIv3[0] = (byte) 0x00;
    testIv3[1] = (byte) 0xFF;
    testIv3[2] = (byte) 0xFE;

    // 00000000 11111111 11111111
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x00, (byte) 0xFF, (byte) 0xFF});

    // 00000001 00000000 00000000
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x01, (byte) 0x00, (byte) 0x00});

    // 00000001 00000000 00000001
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x01, (byte) 0x00, (byte) 0x01});

    // 11111111 11111111 11111110
    testIv3[0] = (byte) 0xFF;
    testIv3[1] = (byte) 0xFF;
    testIv3[2] = (byte) 0xFE;

    // 11111111 11111111 11111111
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});

    // 00000000 00000000 00000000
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x00});

    // 00000000 00000000 00000001
    CryptoModuleParameters.incrementIV(testIv3, testIv3.length - 1);
    assertArrayEquals(testIv3, new byte[] {(byte) 0x00, (byte) 0x00, (byte) 0x01});

  }

  /**
   * Used in AESGCM unit tests to encrypt data. Uses MARKER_STRING and MARKER_INT
   *
   * @param conf
   *          The accumulo configuration
   * @param initVector
   *          The IV to be used in encryption
   * @return the encrypted string
   * @throws IOException
   *           if DataOutputStream fails
   */
  private static byte[] testEncryption(AccumuloConfiguration conf, byte[] initVector) throws IOException {
    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);
    params.getAllOptions().put(Property.CRYPTO_WAL_CIPHER_SUITE.getKey(), "AES/GCM/NoPadding");
    params.setInitializationVector(initVector);

    /*
     * Now lets encrypt this data!
     */
    ByteArrayOutputStream encryptedByteStream = new ByteArrayOutputStream();
    params.setPlaintextOutputStream(new NoFlushOutputStream(encryptedByteStream));
    params = cryptoModule.getEncryptingOutputStream(params);
    DataOutputStream encryptedDataStream = new DataOutputStream(params.getEncryptedOutputStream());
    encryptedDataStream.writeUTF(MARKER_STRING);
    encryptedDataStream.writeInt(MARKER_INT);
    encryptedDataStream.close();
    byte[] encryptedBytes = encryptedByteStream.toByteArray();
    return (encryptedBytes);
  }

  /**
   * Used in AESGCM unit tests to decrypt data. Uses MARKER_STRING and MARKER_INT
   *
   * @param conf
   *          The accumulo configuration
   * @param encryptedBytes
   *          The encrypted bytes
   * @return 0 if data is incorrectly decrypted, 1 if decrypted data matches input
   * @throws IOException
   *           if DataInputStream fails
   * @throws AEADBadTagException
   *           if the encrypted stream has been modified
   */
  private static Integer testDecryption(AccumuloConfiguration conf, byte[] encryptedBytes) throws IOException, AEADBadTagException {
    CryptoModuleParameters params = CryptoModuleFactory.createParamsObjectFromAccumuloConfiguration(conf);
    CryptoModule cryptoModule = CryptoModuleFactory.getCryptoModule(conf);
    ByteArrayInputStream decryptedByteStream = new ByteArrayInputStream(encryptedBytes);
    params.setEncryptedInputStream(decryptedByteStream);
    params = cryptoModule.getDecryptingInputStream(params);
    DataInputStream decryptedDataStream = new DataInputStream(params.getPlaintextInputStream());

    String utf;
    Integer in;
    try {
      utf = decryptedDataStream.readUTF();
      in = decryptedDataStream.readInt();
    } catch (IOException e) {
      if (e.getCause().getClass().equals(AEADBadTagException.class)) {
        throw new AEADBadTagException();
      } else {
        throw e;
      }
    }

    decryptedDataStream.close();
    if (utf.equals(MARKER_STRING) && in.equals(MARKER_INT)) {
      return 1;
    } else {
      return 0;
    }
  }

}
