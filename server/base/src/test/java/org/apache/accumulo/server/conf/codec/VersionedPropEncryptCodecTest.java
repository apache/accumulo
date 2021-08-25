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
package org.apache.accumulo.server.conf.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionedPropEncryptCodecTest {

  private final Logger log = LoggerFactory.getLogger(VersionedPropEncryptCodecTest.class);

  /**
   * Perform a round trip - encode, decode set of operations.
   *
   * @throws Exception
   *           an exception is a test failure.
   */
  @Test
  public void roundTripSample() throws Exception {

    // set-up sample "secret" key - for testing only.
    final char[] pass = {'a', 'b', 'c'};
    final byte[] salt = {1, 2, 3};

    VersionedPropEncryptCodec.GCMCipherParams cipherProps =
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt);

    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(Cipher.ENCRYPT_MODE, cipherProps.getSecretKey(), cipherProps.getParameterSpec());

    byte[] payload;

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

      CipherOutputStream cos = new CipherOutputStream(bos, cipher);

      DataOutputStream dos = new DataOutputStream(cos);

      dos.writeUTF("A");
      dos.writeUTF("B");
      dos.writeUTF("C");

      cos.flush();
      cos.close();

      payload = bos.toByteArray();

      log.debug("Output: {}", payload);

    }

    cipher.init(Cipher.DECRYPT_MODE, cipherProps.getSecretKey(), cipherProps.getParameterSpec());
    try (ByteArrayInputStream bis = new ByteArrayInputStream(payload)) {

      // write the property map keys, values.
      try (CipherInputStream cis = new CipherInputStream(bis, cipher);

          DataInputStream cdis = new DataInputStream(cis)) {

        assertEquals("A", cdis.readUTF());
        assertEquals("B", cdis.readUTF());
        assertEquals("C", cdis.readUTF());
      }
    }
  }

  /**
   * Validate versioning with something other than default.
   */
  @Test
  public void roundTripEncryption() throws Exception {

    int aVersion = 13;
    Instant now = Instant.now();

    Map<String,String> p = new HashMap<>();
    p.put("k1", "v1");

    VersionedProperties vProps = new VersionedProperties(aVersion, now, p);

    final char[] pass = {'a', 'b', 'c'};
    final byte[] salt = {1, 2, 3};

    VersionedPropCodec encoder = VersionedPropEncryptCodec.codec(false,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    byte[] encodedBytes = encoder.toBytes(vProps);

    log.debug("Encoded: {}", encodedBytes);

    VersionedProperties decodedProps = encoder.fromBytes(encodedBytes);

    log.debug("Decoded: {}", decodedProps.print(true));

    assertEquals(vProps.getProperties(), decodedProps.getProperties());

    // validate that the expected node version matches original version.
    assertEquals(aVersion, vProps.getDataVersion());

    // validate encoded version incremented.
    assertEquals(aVersion + 1, decodedProps.getDataVersion());

    assertEquals("encoded version should be 1 up", aVersion + 1, decodedProps.getDataVersion());
    assertEquals("version written should be the source next version", vProps.getNextVersion(),
        decodedProps.getDataVersion());
    assertEquals("the next version in decoded should be +2", aVersion + 2,
        decodedProps.getNextVersion());

    assertTrue("timestamp should be now or earlier",
        vProps.getTimestamp().compareTo(Instant.now()) <= 0);

  }

  /**
   * Validate versioning with something other than default.
   */
  @Test
  public void roundTripEncryptionCompressed() throws Exception {

    int aVersion = 13;
    Instant now = Instant.now();

    // compression friendly
    Map<String,String> p = new HashMap<>();
    p.put("accumulo.prop.key_name.1", "value1");
    p.put("accumulo.prop.key_name.2", "value2");
    p.put("accumulo.prop.key_name.3", "value3");
    p.put("accumulo.prop.key_name.4", "value4");
    p.put("accumulo.prop.key_name.5", "value5");
    p.put("accumulo.prop.key_name.6", "value9");

    VersionedProperties vProps = new VersionedProperties(aVersion, now, p);

    final char[] pass = {'a', 'b', 'c'};
    final byte[] salt = {1, 2, 3};

    VersionedPropCodec encoder1 = VersionedPropEncryptCodec.codec(true,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    byte[] encodedBytes = encoder1.toBytes(vProps);

    log.debug("len: {}, bytes: {}", encodedBytes.length, encodedBytes);

    VersionedProperties decodedProps = encoder1.fromBytes(encodedBytes);

    log.debug("Decoded: {}", decodedProps.print(true));

    assertEquals(vProps.getProperties(), decodedProps.getProperties());

    // validate that the expected node version matches original version.
    assertEquals(aVersion, vProps.getDataVersion());

    // validate encoded version incremented.
    assertEquals(aVersion + 1, decodedProps.getDataVersion());

    assertEquals("encoded version should be 1 up", aVersion + 1, decodedProps.getDataVersion());
    assertEquals("version written should be the source next version", vProps.getNextVersion(),
        decodedProps.getDataVersion());
    assertEquals("the next version in decoded should be +2", aVersion + 2,
        decodedProps.getNextVersion());

    assertTrue("timestamp should be now or earlier",
        vProps.getTimestamp().compareTo(Instant.now()) <= 0);

  }

  @Test
  public void validateEncryptedValuesChange() throws Exception {

    int aVersion = 13;
    Instant now = Instant.now();

    Map<String,String> p = new HashMap<>();
    p.put("k1", "v1");

    VersionedProperties vProps = new VersionedProperties(aVersion, now, p);

    final char[] pass = {'a', 'b', 'c'};
    final byte[] salt = {1, 2, 3};

    VersionedPropCodec codec1 = VersionedPropEncryptCodec.codec(false,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    byte[] encodedBytes1 = codec1.toBytes(vProps);

    VersionedPropCodec codec2 = VersionedPropEncryptCodec.codec(false,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    byte[] encodedBytes2 = codec2.toBytes(vProps);

    log.debug("Encoded: {}", encodedBytes1);
    log.debug("Encoded: {}", encodedBytes2);

    VersionedProperties from2 = codec1.fromBytes(encodedBytes2);
    VersionedProperties from1 = codec2.fromBytes(encodedBytes1);

    assertEquals(from1.getProperties(), from2.getProperties());

    VersionedPropCodec codec3 = VersionedPropEncryptCodec.codec(false,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    VersionedProperties from3 = codec3.fromBytes(encodedBytes1);
    assertEquals(from1.getDataVersion(), from3.getDataVersion());
    assertEquals(from1.getProperties(), from3.getProperties());

    assertNotEquals(encodedBytes1, encodedBytes2);

  }

  private String keyGen() throws NoSuchAlgorithmException {
    SecretKey secretKey = KeyGenerator.getInstance("AES").generateKey();
    return Base64.getEncoder().encodeToString(secretKey.getEncoded());
  }
}
