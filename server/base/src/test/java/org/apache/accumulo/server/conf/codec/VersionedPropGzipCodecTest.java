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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

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
}
