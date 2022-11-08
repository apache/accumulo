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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Exercise the base class specific methods - most testing will occur in subclasses
 */
public class VersionedPropCodecTest {

  @Test
  public void invalidEncodingNullArray() {
    assertThrows(IllegalArgumentException.class, () -> VersionedPropCodec.getEncodingVersion(null));
  }

  @Test
  public void validEncoding() {
    // length so that array reads do not error
    byte[] bytes = new byte[100];
    int encodingVersion = VersionedPropCodec.getEncodingVersion(bytes);
    assertEquals(0, encodingVersion);
  }

  /**
   * The timestamp will be invalid - this should cause a timestamp parse error that will be remapped
   * to an IllegalArgumentException.
   */
  @Test
  public void getDataVersionBadTimestamp() {
    // length so that array reads do not error
    byte[] bytes = new byte[100];
    assertThrows(IllegalArgumentException.class, () -> VersionedPropCodec.readTimestamp(bytes));
  }

  @Test
  public void goPath() throws IOException {
    int aVersion = 13;
    var timestamp = Instant.now();
    VersionedProperties vProps = new VersionedProperties(aVersion, timestamp, Map.of("k1", "v1"));

    VersionedPropCodec codec = VersionedPropGzipCodec.codec(true);
    byte[] encodedBytes = codec.toBytes(vProps);

    assertEquals(timestamp, VersionedPropCodec.readTimestamp(encodedBytes));
  }
}
