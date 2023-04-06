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
package org.apache.accumulo.core.crypto;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.apache.accumulo.core.conf.Property.INSTANCE_CRYPTO_FACTORY;
import static org.apache.accumulo.core.crypto.CryptoUtils.getFileDecrypter;
import static org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope.TABLE;
import static org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope.WAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.streams.NoFlushOutputStream;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.spi.crypto.AESCryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoService.CryptoException;
import org.apache.accumulo.core.spi.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.FileDecrypter;
import org.apache.accumulo.core.spi.crypto.FileEncrypter;
import org.apache.accumulo.core.spi.crypto.GenericCryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.NoCryptoService;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.core.spi.crypto.PerTableCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CryptoTest {

  private static final SecureRandom random = new SecureRandom();
  private static final int MARKER_INT = 0xCADEFEDD;
  private static final String MARKER_STRING = "1 2 3 4 5 6 7 8 a b c d e f g h ";
  private static final Configuration hadoopConf = new Configuration();

  public enum ConfigMode {
    CRYPTO_OFF, CRYPTO_TABLE_ON, CRYPTO_WAL_ON, CRYPTO_TABLE_ON_DISABLED, CRYPTO_WAL_ON_DISABLED
  }

  @BeforeAll
  public static void setupKeyFiles() throws IOException {
    setupKeyFiles(CryptoTest.class);
  }

  public static void setupKeyFiles(Class<?> testClass) throws IOException {
    FileSystem fs = FileSystem.getLocal(hadoopConf);
    Path aesPath = new Path(keyPath(testClass));
    try (FSDataOutputStream out = fs.create(aesPath)) {
      out.writeUTF("sixteenbytekey"); // 14 + 2 from writeUTF
    }
    try (FSDataOutputStream out = fs.create(new Path(emptyKeyPath(testClass)))) {
      // auto close after creating
      assertNotNull(out);
    }
  }

  public static ConfigurationCopy getAccumuloConfig(ConfigMode configMode, Class<?> testClass) {
    ConfigurationCopy cfg = new ConfigurationCopy(DefaultConfiguration.getInstance());
    switch (configMode) {
      case CRYPTO_TABLE_ON_DISABLED:
        cfg.set(INSTANCE_CRYPTO_FACTORY, PerTableCryptoServiceFactory.class.getName());
        cfg.set(PerTableCryptoServiceFactory.TABLE_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "false");
        break;
      case CRYPTO_TABLE_ON:
        cfg.set(INSTANCE_CRYPTO_FACTORY, PerTableCryptoServiceFactory.class.getName());
        cfg.set(PerTableCryptoServiceFactory.TABLE_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "true");
        break;
      case CRYPTO_WAL_ON_DISABLED:
        cfg.set(INSTANCE_CRYPTO_FACTORY, GenericCryptoServiceFactory.class.getName());
        cfg.set(GenericCryptoServiceFactory.GENERAL_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "false");
        break;
      case CRYPTO_WAL_ON:
        cfg.set(INSTANCE_CRYPTO_FACTORY, GenericCryptoServiceFactory.class.getName());
        cfg.set(GenericCryptoServiceFactory.GENERAL_SERVICE_NAME_PROP,
            AESCryptoService.class.getName());
        cfg.set(AESCryptoService.KEY_URI_PROPERTY, CryptoTest.keyPath(testClass));
        cfg.set(AESCryptoService.ENCRYPT_ENABLED_PROPERTY, "true");
        break;
      case CRYPTO_OFF:
        break;
    }
    return cfg;
  }

  private ConfigurationCopy getAccumuloConfig(ConfigMode configMode) {
    return getAccumuloConfig(configMode, getClass());
  }

  private Map<String,String> getAllCryptoProperties(ConfigMode configMode) {
    var cc = getAccumuloConfig(configMode);
    return cc.getAllCryptoProperties();
  }

  public static String keyPath(Class<?> testClass) {
    return System.getProperty("user.dir") + "/target/" + testClass.getSimpleName() + "-testkeyfile";
  }

  public static String emptyKeyPath(Class<?> testClass) {
    return System.getProperty("user.dir") + "/target/" + testClass.getSimpleName()
        + "-emptykeyfile";
  }

  @Test
  public void simpleGCMTest() throws Exception {
    AESCryptoService cs = new AESCryptoService();
    cs.init(getAllCryptoProperties(ConfigMode.CRYPTO_TABLE_ON));
    CryptoEnvironment encEnv = new CryptoEnvironmentImpl(TABLE, null, null);
    FileEncrypter encrypter = cs.getFileEncrypter(encEnv);
    byte[] params = encrypter.getDecryptionParameters();
    assertNotNull(params);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    CryptoUtils.writeParams(params, dataOut);
    OutputStream encrypted = encrypter.encryptStream(dataOut);

    assertNotNull(encrypted);
    DataOutputStream cipherOut = new DataOutputStream(encrypted);

    cipherOut.writeUTF(MARKER_STRING);

    cipherOut.close();
    dataOut.close();
    encrypted.close();
    out.close();

    byte[] cipherText = out.toByteArray();

    // decrypt
    ByteArrayInputStream in = new ByteArrayInputStream(cipherText);
    FileDecrypter decrypter = getFileDecrypter(cs, TABLE, null, new DataInputStream(in));
    DataInputStream decrypted = new DataInputStream(decrypter.decryptStream(in));
    String plainText = decrypted.readUTF();
    decrypted.close();
    in.close();

    assertEquals(MARKER_STRING, plainText);
  }

  @Test
  public void testAESCryptoServiceWAL() throws Exception {
    AESCryptoService cs = new AESCryptoService();
    cs.init(getAllCryptoProperties(ConfigMode.CRYPTO_WAL_ON));

    byte[] resultingBytes = encrypt(cs, Scope.WAL);

    String stringifiedBytes = Arrays.toString(resultingBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(null, MARKER_STRING, MARKER_INT);

    assertNotEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(cs, resultingBytes, Scope.WAL);
  }

  /**
   * AESCryptoService is configured but only for reading
   */
  @Test
  public void testAESCryptoServiceWALDisabled() throws Exception {
    AESCryptoService csEnabled = new AESCryptoService();
    AESCryptoService csDisabled = new AESCryptoService();
    csEnabled.init(getAllCryptoProperties(ConfigMode.CRYPTO_WAL_ON));
    csDisabled.init(getAllCryptoProperties(ConfigMode.CRYPTO_WAL_ON_DISABLED));

    // make sure we can read encrypted
    byte[] encryptedBytes = encrypt(csEnabled, Scope.WAL);
    String stringEncryptedBytes = Arrays.toString(encryptedBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(null, MARKER_STRING, MARKER_INT);
    assertNotEquals(stringEncryptedBytes, stringifiedMarkerBytes);
    decrypt(csDisabled, encryptedBytes, Scope.WAL);

    // make sure we don't encrypt when disabled
    byte[] plainBytes = encrypt(csDisabled, Scope.WAL);
    String stringPlainBytes = Arrays.toString(plainBytes);
    assertNotEquals(stringEncryptedBytes, stringPlainBytes);
    decrypt(csDisabled, plainBytes, Scope.WAL);
  }

  @Test
  public void testAESCryptoServiceRFILE() throws Exception {
    AESCryptoService cs = new AESCryptoService();
    cs.init(getAllCryptoProperties(ConfigMode.CRYPTO_TABLE_ON));

    byte[] resultingBytes = encrypt(cs, TABLE);

    String stringifiedBytes = Arrays.toString(resultingBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(null, MARKER_STRING, MARKER_INT);

    assertNotEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(cs, resultingBytes, TABLE);
  }

  /**
   * AESCryptoService is configured but only for reading
   */
  @Test
  public void testAESCryptoServiceTableDisabled() throws Exception {
    AESCryptoService csEnabled = new AESCryptoService();
    AESCryptoService csDisabled = new AESCryptoService();
    csEnabled.init(getAllCryptoProperties(ConfigMode.CRYPTO_TABLE_ON));
    csDisabled.init(getAllCryptoProperties(ConfigMode.CRYPTO_TABLE_ON_DISABLED));

    // make sure we can read encrypted
    byte[] encryptedBytes = encrypt(csEnabled, TABLE);
    String stringEncryptedBytes = Arrays.toString(encryptedBytes);
    String stringifiedMarkerBytes = getStringifiedBytes(null, MARKER_STRING, MARKER_INT);
    assertNotEquals(stringEncryptedBytes, stringifiedMarkerBytes);
    decrypt(csDisabled, encryptedBytes, TABLE);

    // make sure we don't encrypt when disabled
    byte[] plainBytes = encrypt(csDisabled, TABLE);
    String stringPlainBytes = Arrays.toString(plainBytes);
    assertNotEquals(stringEncryptedBytes, stringPlainBytes);
    decrypt(csDisabled, plainBytes, TABLE);
  }

  @Test
  public void testNoEncryptionWAL() throws Exception {
    CryptoService cs = NoCryptoServiceFactory.NONE;
    byte[] encryptedBytes = encrypt(cs, Scope.WAL);

    String stringifiedBytes = Arrays.toString(encryptedBytes);
    String stringifiedMarkerBytes =
        getStringifiedBytes("U+1F47B".getBytes(), MARKER_STRING, MARKER_INT);

    assertEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(cs, encryptedBytes, Scope.WAL);
  }

  @Test
  public void testNoEncryptionRFILE() throws Exception {
    CryptoService cs = new NoCryptoService();
    byte[] encryptedBytes = encrypt(cs, TABLE);

    String stringifiedBytes = Arrays.toString(encryptedBytes);
    String stringifiedMarkerBytes =
        getStringifiedBytes("U+1F47B".getBytes(), MARKER_STRING, MARKER_INT);

    assertEquals(stringifiedBytes, stringifiedMarkerBytes);

    decrypt(cs, encryptedBytes, TABLE);
  }

  @Test
  public void testRFileClientEncryption() throws Exception {
    AccumuloConfiguration cryptoOnConf = getAccumuloConfig(ConfigMode.CRYPTO_TABLE_ON);
    FileSystem fs = FileSystem.getLocal(hadoopConf);
    ArrayList<Key> keys = testData();
    SummarizerConfiguration sumConf =
        SummarizerConfiguration.builder(KeyCounter.class.getName()).build();

    String file = "target/testFile1.rf";
    fs.delete(new Path(file), true);
    try (RFileWriter writer = RFile.newWriter().to(file).withFileSystem(fs)
        .withTableProperties(cryptoOnConf).withSummarizers(sumConf).build()) {
      Value empty = new Value();
      writer.startDefaultLocalityGroup();
      for (Key key : keys) {
        writer.append(key, empty);
      }
    }

    // test to make sure the RFile is encrypted
    ArrayList<Key> keysRead = new ArrayList<>();
    try (Scanner iter = RFile.newScanner().from(file).withFileSystem(fs).build()) {
      assertThrows(UncheckedIOException.class, () -> iter.forEach(e -> keysRead.add(e.getKey())),
          "The file was expected to be encrypted but was not");
      assertEquals(0, keysRead.size());
    }

    keysRead.clear();
    try (Scanner iter = RFile.newScanner().from(file).withFileSystem(fs)
        .withTableProperties(cryptoOnConf).build()) {
      iter.forEach(e -> keysRead.add(e.getKey()));
    }
    assertEquals(keys, keysRead);

    Collection<Summary> summaries =
        RFile.summaries().from(file).withFileSystem(fs).withTableProperties(cryptoOnConf).read();
    Summary summary = summaries.stream().collect(onlyElement());
    assertEquals(keys.size(), (long) summary.getStatistics().get("keys"));
    assertEquals(1, summary.getStatistics().size());
    assertEquals(0, summary.getFileStatistics().getInaccurate());
    assertEquals(1, summary.getFileStatistics().getTotal());
  }

  @Test
  // This test is to ensure when Crypto is configured that it can read unencrypted files
  public void testReadNoCryptoWithCryptoConfigured() throws Exception {
    AccumuloConfiguration cryptoOffConf = getAccumuloConfig(ConfigMode.CRYPTO_OFF);
    AccumuloConfiguration cryptoOnConf = getAccumuloConfig(ConfigMode.CRYPTO_TABLE_ON);
    FileSystem fs = FileSystem.getLocal(hadoopConf);
    ArrayList<Key> keys = testData();

    String file = "target/testFile2.rf";
    fs.delete(new Path(file), true);
    try (RFileWriter writer =
        RFile.newWriter().to(file).withFileSystem(fs).withTableProperties(cryptoOffConf).build()) {
      Value empty = new Value();
      writer.startDefaultLocalityGroup();
      for (Key key : keys) {
        writer.append(key, empty);
      }
    }

    ArrayList<Key> keysRead;
    try (Scanner iter = RFile.newScanner().from(file).withFileSystem(fs)
        .withTableProperties(cryptoOnConf).build()) {
      keysRead = new ArrayList<>();
      iter.forEach(e -> keysRead.add(e.getKey()));
    }
    assertEquals(keys, keysRead);
  }

  @Test
  public void testMissingConfigProperties() throws ReflectiveOperationException {
    var cryptoProps = getAllCryptoProperties(ConfigMode.CRYPTO_TABLE_ON);
    var droppedProperty = cryptoProps.remove(AESCryptoService.KEY_URI_PROPERTY);
    assertNotNull(droppedProperty);

    String configuredClass = cryptoProps.get(INSTANCE_CRYPTO_FACTORY.getKey());
    CryptoEnvironment env = new CryptoEnvironmentImpl(TABLE, TableId.of("5"), null);
    Class<? extends CryptoServiceFactory> clazz =
        ClassLoaderUtil.loadClass(configuredClass, CryptoServiceFactory.class);
    CryptoServiceFactory factory = clazz.getDeclaredConstructor().newInstance();

    assertThrows(NullPointerException.class, () -> factory.getService(env, cryptoProps));

    CryptoEnvironment env2 = new CryptoEnvironmentImpl(WAL);
    var cryptoProps2 = getAllCryptoProperties(ConfigMode.CRYPTO_WAL_ON);
    droppedProperty = cryptoProps2.remove(GenericCryptoServiceFactory.GENERAL_SERVICE_NAME_PROP);
    assertNotNull(droppedProperty);

    assertThrows(NullPointerException.class, () -> factory.getService(env2, cryptoProps2));
  }

  @Test
  public void testAESKeyUtilsGeneratesKey() throws NoSuchAlgorithmException,
      NoSuchProviderException, NoSuchPaddingException, InvalidKeyException {
    // verify valid key sizes (corresponds to 128, 192, and 256 bits)
    for (int i : new int[] {16, 24, 32}) {
      verifyKeySizeForCBC(random, i);
    }
    // verify invalid key sizes
    for (int i : new int[] {1, 2, 8, 11, 15, 64, 128}) {
      assertThrows(InvalidKeyException.class, () -> verifyKeySizeForCBC(random, i));
    }
  }

  // this has to be a separate method, for spotbugs, because spotbugs annotation doesn't seem to
  // apply to the lambda inline
  @SuppressFBWarnings(value = "CIPHER_INTEGRITY", justification = "CBC is being tested")
  private void verifyKeySizeForCBC(SecureRandom sr, int sizeInBytes)
      throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
    java.security.Key key = AESCryptoService.generateKey(sr, sizeInBytes);
    Cipher.getInstance("AES/CBC/NoPadding").init(Cipher.ENCRYPT_MODE, key);
  }

  @Test
  public void testAESKeyUtilsWrapAndUnwrap()
      throws NoSuchAlgorithmException, NoSuchProviderException {
    java.security.Key kek = AESCryptoService.generateKey(random, 16);
    java.security.Key fek = AESCryptoService.generateKey(random, 16);
    byte[] wrapped = AESCryptoService.wrapKey(fek, kek);
    assertFalse(Arrays.equals(fek.getEncoded(), wrapped));
    java.security.Key unwrapped = AESCryptoService.unwrapKey(wrapped, kek);
    assertEquals(unwrapped, fek);
  }

  @Test
  public void testAESKeyUtilsFailUnwrapWithWrongKEK()
      throws NoSuchAlgorithmException, NoSuchProviderException {
    java.security.Key kek = AESCryptoService.generateKey(random, 16);
    java.security.Key fek = AESCryptoService.generateKey(random, 16);
    byte[] wrongBytes = kek.getEncoded();
    wrongBytes[0]++;
    java.security.Key wrongKek = new SecretKeySpec(wrongBytes, "AES");

    byte[] wrapped = AESCryptoService.wrapKey(fek, kek);
    assertThrows(CryptoException.class, () -> AESCryptoService.unwrapKey(wrapped, wrongKek));
  }

  @Test
  public void testAESKeyUtilsLoadKekFromUri() throws IOException {
    java.security.Key fileKey = AESCryptoService.loadKekFromUri(keyPath(getClass()));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeUTF("sixteenbytekey");
    SecretKeySpec handKey = new SecretKeySpec(baos.toByteArray(), "AES");
    assertEquals(fileKey, handKey);
  }

  @Test
  public void testAESKeyUtilsLoadKekFromUriInvalidUri() {
    assertThrows(CryptoException.class, () -> AESCryptoService.loadKekFromUri(
        System.getProperty("user.dir") + "/target/CryptoTest-testkeyfile-doesnt-exist"));
  }

  @Test
  public void testAESKeyUtilsLoadKekFromEmptyFile() {
    assertThrows(CryptoException.class,
        () -> AESCryptoService.loadKekFromUri(emptyKeyPath(getClass())));
  }

  @Test
  public void testPerTableFactory() {
    PerTableCryptoServiceFactory factory = new PerTableCryptoServiceFactory();
    CryptoEnvironment env = new CryptoEnvironmentImpl(TABLE, TableId.of("5"), null);
    HashMap<String,String> props = new HashMap<>();

    // empty properties returns NoCrypto
    CryptoService cs = factory.getService(env, props);
    assertEquals(NoCryptoService.class, cs.getClass());

    var config = getAccumuloConfig(ConfigMode.CRYPTO_TABLE_ON);
    props.putAll(config.getAllCryptoProperties());
    cs = factory.getService(env, props);
    assertEquals(AESCryptoService.class, cs.getClass());

    CryptoEnvironment env2 = new CryptoEnvironmentImpl(TABLE, TableId.of("6"), null);
    props.put(PerTableCryptoServiceFactory.TABLE_SERVICE_NAME_PROP,
        NoCryptoService.class.getName());
    cs = factory.getService(env2, props);
    assertEquals(NoCryptoService.class, cs.getClass());

    assertEquals(2, factory.getCount());
  }

  private ArrayList<Key> testData() {
    ArrayList<Key> keys = new ArrayList<>();
    keys.add(new Key("a", "cf", "cq"));
    keys.add(new Key("a1", "cf", "cq"));
    keys.add(new Key("a2", "cf", "cq"));
    keys.add(new Key("a3", "cf", "cq"));
    return keys;
  }

  private <C extends CryptoService> byte[] encrypt(C cs, Scope scope) throws Exception {
    CryptoEnvironment env = new CryptoEnvironmentImpl(scope, null, null);
    FileEncrypter encrypter = cs.getFileEncrypter(env);
    byte[] params = encrypter.getDecryptionParameters();

    assertNotNull(encrypter, "CryptoService returned null FileEncrypter");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    CryptoUtils.writeParams(params, dataOut);
    DataOutputStream encrypted =
        new DataOutputStream(encrypter.encryptStream(new NoFlushOutputStream(dataOut)));
    assertNotNull(encrypted);

    encrypted.writeUTF(MARKER_STRING);
    encrypted.writeInt(MARKER_INT);
    encrypted.close();
    dataOut.close();
    out.close();
    return out.toByteArray();
  }

  private void decrypt(CryptoService cs, byte[] resultingBytes, Scope scope) throws Exception {
    try (DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(resultingBytes))) {
      FileDecrypter decrypter = getFileDecrypter(cs, scope, null, dataIn);

      try (DataInputStream decrypted = new DataInputStream(decrypter.decryptStream(dataIn))) {
        String markerString = decrypted.readUTF();
        int markerInt = decrypted.readInt();

        assertEquals(MARKER_STRING, markerString);
        assertEquals(MARKER_INT, markerInt);
      }
    }
  }

  private String getStringifiedBytes(byte[] params, String s, int i) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);

    if (params != null) {
      dataOut.writeInt(params.length);
      dataOut.write(params);
    }
    dataOut.writeUTF(s);
    dataOut.writeInt(i);
    dataOut.close();
    byte[] stringMarkerBytes = out.toByteArray();
    return Arrays.toString(stringMarkerBytes);
  }

  // simple counter to just make sure crypto works with summaries
  public static class KeyCounter implements Summarizer {
    @Override
    public Collector collector(SummarizerConfiguration sc) {
      return new Collector() {

        long keys = 0;

        @Override
        public void accept(Key k, Value v) {
          if (!k.isDeleted()) {
            keys++;
          }
        }

        @Override
        public void summarize(StatisticConsumer sc) {
          sc.accept("keys", keys);
        }
      };
    }

    @Override
    public Combiner combiner(SummarizerConfiguration sc) {
      return (m1, m2) -> m2.forEach((k, v) -> m1.merge(k, v, Long::sum));
    }
  }

}
