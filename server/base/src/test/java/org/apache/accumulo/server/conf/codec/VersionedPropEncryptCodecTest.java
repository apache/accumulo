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
package org.apache.accumulo.server.conf.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Instant;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionedPropEncryptCodecTest {

  private final Logger log = LoggerFactory.getLogger(VersionedPropEncryptCodecTest.class);

  /**
   * Perform a round trip - encode, decode set of operations.
   *
   * @throws Exception an exception is a test failure.
   */
  @Test
  public void roundTripSample() throws Exception {

    // set-up sample "secret" key - for testing only.
    final char[] pass = {'a', 'b', 'c'};
    final byte[] salt = {1, 2, 3};

    var cipherProps = new VersionedPropEncryptCodec.GCMCipherParams(pass, salt);

    var cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(Cipher.ENCRYPT_MODE, cipherProps.getSecretKey(), cipherProps.getParameterSpec());

    byte[] payload;

    try (var bos = new ByteArrayOutputStream()) {
      var cos = new CipherOutputStream(bos, cipher);
      var dos = new DataOutputStream(cos);

      dos.writeUTF("A");
      dos.writeUTF("B");
      dos.writeUTF("C");

      cos.close();

      payload = bos.toByteArray();

      log.debug("Output: {}", payload);

    }

    cipher.init(Cipher.DECRYPT_MODE, cipherProps.getSecretKey(), cipherProps.getParameterSpec());

    try (var bis = new ByteArrayInputStream(payload)) {
      // write the property map keys, values.
      try (var cis = new CipherInputStream(bis, cipher);
          var cdatastream = new DataInputStream(cis)) {

        assertEquals("A", cdatastream.readUTF());
        assertEquals("B", cdatastream.readUTF());
        assertEquals("C", cdatastream.readUTF());
      }
    }
  }

  /**
   * Validate versioning with something other than default.
   */
  @Test
  public void roundTripEncryption() throws Exception {

    int aVersion = 13;

    VersionedProperties vProps =
        new VersionedProperties(aVersion, Instant.now(), Map.of("k1", "v1"));

    final char[] pass = {'a', 'b', 'c'};
    final byte[] salt = {1, 2, 3};

    var encoder = VersionedPropEncryptCodec.codec(false,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    byte[] encodedBytes = encoder.toBytes(vProps);

    log.debug("Encoded: {}", encodedBytes);

    // store would increment version on write
    VersionedProperties decodedProps = encoder.fromBytes(aVersion + 1, encodedBytes);

    log.debug("Decoded: {}", decodedProps.print(true));

    assertEquals(vProps.asMap(), decodedProps.asMap());

    // validate that the expected node version matches original version.
    assertEquals(aVersion, vProps.getDataVersion());

    // validate encoded version incremented.
    assertEquals(aVersion + 1, decodedProps.getDataVersion());

    assertEquals(aVersion + 1, decodedProps.getDataVersion(), "encoded version should be 1 up");

    assertTrue(vProps.getTimestamp().compareTo(Instant.now()) <= 0,
        "timestamp should be now or earlier");

  }

  /**
   * Validate versioning with something other than default.
   */
  @Test
  public void roundTripEncryptionCompressed() throws Exception {

    int aVersion = 13;
    Instant now = Instant.now();

    // compression friendly
    // @formatter:off
    Map<String, String> p
        = Map.of("accumulo.prop.key_name.1", "value1", "accumulo.prop.key_name.2",
            "value2", "accumulo.prop.key_name.3", "value3", "accumulo.prop.key_name.4", "value4",
            "accumulo.prop.key_name.5", "value5", "accumulo.prop.key_name.6", "value9");
    // @@formatter:on
    VersionedProperties vProps = new VersionedProperties(aVersion, now, p);

    final char[] pass = {'a', 'b', 'c'};
    final byte[] salt = {1, 2, 3};

    VersionedPropCodec encoder1 = VersionedPropEncryptCodec.codec(true,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    byte[] encodedBytes = encoder1.toBytes(vProps);

    log.debug("len: {}, bytes: {}", encodedBytes.length, encodedBytes);

    // store would increment version on write
    VersionedProperties decodedProps = encoder1.fromBytes(aVersion + 1, encodedBytes);

    log.debug("Decoded: {}", decodedProps.print(true));

    assertEquals(vProps.asMap(), decodedProps.asMap());

    // validate that the expected node version matches original version.
    assertEquals(aVersion, vProps.getDataVersion());

    // validate encoded version incremented.
    assertEquals(aVersion + 1, decodedProps.getDataVersion());

    assertEquals(aVersion + 1, decodedProps.getDataVersion(), "encoded version should be 1 up");

    assertTrue(vProps.getTimestamp().compareTo(Instant.now()) <= 0,
        "timestamp should be now or earlier");

  }

  @Test
  public void validateEncryptedValuesChange() throws Exception {

    int aVersion = 13;

    VersionedProperties vProps =
        new VersionedProperties(aVersion, Instant.now(), Map.of("k1", "v1"));

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

    VersionedProperties from2 = codec1.fromBytes(0, encodedBytes2);
    VersionedProperties from1 = codec2.fromBytes(0, encodedBytes1);

    assertEquals(from1.asMap(), from2.asMap());

    VersionedPropCodec codec3 = VersionedPropEncryptCodec.codec(false,
        new VersionedPropEncryptCodec.GCMCipherParams(pass, salt));

    VersionedProperties from3 = codec3.fromBytes(0, encodedBytes1);
    assertEquals(from1.getDataVersion(), from3.getDataVersion());
    assertEquals(from1.asMap(), from3.asMap());

    assertNotEquals(encodedBytes1, encodedBytes2);

  }
}
