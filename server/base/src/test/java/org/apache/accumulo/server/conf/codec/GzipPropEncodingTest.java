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

import static org.apache.accumulo.server.conf.codec.EncodingOptions.COMPRESSED_V1;
import static org.apache.accumulo.server.conf.codec.EncodingOptions.UNCOMPRESSED_V1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exercise the {@link PropSerdesEncoderFactory} and the {@link GzipPropEncoding}
 */
public class GzipPropEncodingTest {

  private static final Logger log = LoggerFactory.getLogger(GzipPropEncodingTest.class);

  @Test
  public void roundTripUncompressed() {

    VersionedProperties vProps = new VersionedPropertiesImpl();
    vProps.addProperty("k1", "v1");

    PropSerdesEncoderFactory encoderFactory = new PropSerdesEncoderFactory(UNCOMPRESSED_V1);

    byte[] encodedMapBytes = encoderFactory.getSerdes().toBytes(vProps);

    VersionedProperties decodedProps = encoderFactory.getSerdes().fromBytes(encodedMapBytes);

    log.info("Decoded: {}", decodedProps.getAllProperties());

    // default - first write version should be 0
    assertEquals("default - first write version should be 0", 0, decodedProps.getDataVersion());
    assertEquals("default - first write next version should be 1", 1,
        decodedProps.getNextVersion());
    assertTrue("timestamp should be now or earlier",
        vProps.getTimestamp().compareTo(Instant.now()) <= 0);
    assertEquals(vProps.getAllProperties(), decodedProps.getAllProperties());
  }

  @Test
  public void roundTripCompressed() {

    VersionedProperties vProps = new VersionedPropertiesImpl();
    vProps.addProperty("k1", "v1");

    PropSerdesEncoderFactory encoderFactory = new PropSerdesEncoderFactory(COMPRESSED_V1);

    byte[] encodedMapBytes = encoderFactory.getSerdes().toBytes(vProps);

    VersionedProperties decodedProps = encoderFactory.getSerdes().fromBytes(encodedMapBytes);

    log.info("Decoded: {}", decodedProps.getAllProperties());

    assertEquals("default - first write version should be 0", 0, decodedProps.getDataVersion());
    assertEquals("default - first write next version should be 1", 1,
        decodedProps.getNextVersion());
    assertTrue("timestamp should be now or earlier",
        vProps.getTimestamp().compareTo(Instant.now()) <= 0);
    assertEquals(vProps.getAllProperties(), decodedProps.getAllProperties());
  }

  /**
   * Validate versioning with something other than default.
   */
  @Test
  public void roundTripVersioning() {

    int aVersion = 13;
    Instant now = Instant.now();

    VersionedProperties vProps = new VersionedPropertiesImpl(aVersion, now);
    vProps.addProperty("k1", "v1");

    PropSerdesEncoderFactory encoderFactory = new PropSerdesEncoderFactory(COMPRESSED_V1);

    byte[] encodedBytes = encoderFactory.getSerdes().toBytes(vProps);

    VersionedProperties decodedProps = encoderFactory.getSerdes().fromBytes(encodedBytes);

    log.trace("Decoded: {}", decodedProps.print(true));

    assertEquals(vProps.getAllProperties(), decodedProps.getAllProperties());

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
}
