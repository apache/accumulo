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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exercise the {@link VersionedPropGzipCodec} class.
 */
public class VersionedPropGzipCodecTest {

  private static final Logger log = LoggerFactory.getLogger(VersionedPropGzipCodecTest.class);

  @Test
  public void roundTripUncompressed() throws IOException {
    VersionedProperties vProps = new VersionedProperties(Map.of("k1", "v1"));

    VersionedPropCodec encoder = VersionedPropGzipCodec.codec(false);

    byte[] encodedMapBytes = encoder.toBytes(vProps);

    VersionedProperties decodedProps = encoder.fromBytes(0, encodedMapBytes);

    log.debug("Decoded: {}", decodedProps.asMap());

    // default - first write version should be 0
    assertEquals(0, decodedProps.getDataVersion(), "default - first write version should be 0");
    assertTrue(vProps.getTimestamp().compareTo(Instant.now()) <= 0,
        "timestamp should be now or earlier");
    assertEquals(vProps.asMap(), decodedProps.asMap());
    assertEquals(vProps.getMetadata(), decodedProps.getMetadata());
  }

  @Test
  public void roundTripCompressed() throws IOException {
    VersionedProperties vProps = new VersionedProperties(Map.of("k1", "v1"));

    VersionedPropCodec codec = VersionedPropGzipCodec.codec(true);

    byte[] encodedMapBytes = codec.toBytes(vProps);

    VersionedProperties decodedProps = codec.fromBytes(0, encodedMapBytes);

    log.debug("Decoded: {}", decodedProps.asMap());

    assertEquals(0, decodedProps.getDataVersion(), "default - first write version should be 0");
    assertTrue(vProps.getTimestamp().compareTo(Instant.now()) <= 0,
        "timestamp should be now or earlier");
    assertEquals(vProps.asMap(), decodedProps.asMap());
    assertEquals(vProps.getMetadata(), decodedProps.getMetadata());
  }

  /**
   * Validate versioning with something other than default.
   */
  @Test
  public void roundTripVersioning() throws IOException {

    int aVersion = 13;
    VersionedProperties vProps =
        new VersionedProperties(aVersion, Instant.now(), Map.of("k1", "v1"));

    VersionedPropCodec codec = VersionedPropGzipCodec.codec(true);
    byte[] encodedBytes = codec.toBytes(vProps);

    VersionedProperties decodedProps = codec.fromBytes(aVersion + 1, encodedBytes);

    log.trace("Decoded: {}", decodedProps.print(true));

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
  public void decodesPayloadWithoutPropertyMetadata() throws IOException {
    VersionedPropCodec codec = VersionedPropGzipCodec.codec(true);
    Instant timestamp = Instant.now();
    byte[] encodedBytes = gzipBytesWithoutPropertyMetadata(timestamp, Map.of("k1", "v1"));

    VersionedProperties decodedProps = codec.fromBytes(0, encodedBytes);

    assertEquals(Map.of("k1", "v1"), decodedProps.asMap());
    assertNotNull(decodedProps.getMetadata().get("k1"));
    assertEquals(timestamp, decodedProps.getMetadata().get("k1").created());
    assertEquals(timestamp, decodedProps.getMetadata().get("k1").modified());
  }

  @Test
  public void roundTrip2() throws IOException {

    int aVersion = 13;
    VersionedProperties vProps =
        new VersionedProperties(aVersion, Instant.now(), Map.of("k1", "v1"));

    VersionedPropCodec codec = VersionedPropGzipCodec.codec(true);
    byte[] encodedBytes = codec.toBytes(vProps);

    VersionedProperties decodedProps = codec.fromBytes(0, encodedBytes);

    log.debug("Decoded: {}", decodedProps.print(true));

    assertEquals(vProps.asMap(), decodedProps.asMap());

  }

  // Write the pre-metadata payload shape so old ZooKeeper data stays readable.
  private static byte[] gzipBytesWithoutPropertyMetadata(Instant timestamp,
      Map<String,String> props) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      EncodingOptions.V1_0(true).encode(dos);
      dos.writeUTF(VersionedProperties.TIMESTAMP_FORMATTER.format(timestamp));
      try (GZIPOutputStream gzipOut = new GZIPOutputStream(bos);
          DataOutputStream zdos = new DataOutputStream(gzipOut)) {
        zdos.writeInt(props.size());
        for (var entry : props.entrySet()) {
          zdos.writeUTF(entry.getKey());
          zdos.writeUTF(entry.getValue());
        }
        gzipOut.finish();
      }
      return bos.toByteArray();
    }
  }
}
