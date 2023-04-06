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
package org.apache.accumulo.core.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.conf.ClientProperty;
import org.junit.jupiter.api.Test;

public class BatchWriterConfigTest {

  @Test
  public void testReasonableDefaults() {
    long expectedMaxMemory = 50 * 1024 * 1024L;
    long expectedMaxLatency = 120000L;
    long expectedTimeout = Long.MAX_VALUE;
    int expectedMaxWriteThreads = 3;
    Durability expectedDurability = Durability.DEFAULT;

    BatchWriterConfig defaults = new BatchWriterConfig();
    assertEquals(expectedMaxMemory, defaults.getMaxMemory());
    assertEquals(expectedMaxLatency, defaults.getMaxLatency(MILLISECONDS));
    assertEquals(expectedTimeout, defaults.getTimeout(MILLISECONDS));
    assertEquals(expectedMaxWriteThreads, defaults.getMaxWriteThreads());
    assertEquals(expectedDurability, defaults.getDurability());
  }

  @Test
  public void testOverridingDefaults() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    bwConfig.setMaxMemory(1123581321L);
    bwConfig.setMaxLatency(22, HOURS);
    bwConfig.setTimeout(33, DAYS);
    bwConfig.setMaxWriteThreads(42);
    bwConfig.setDurability(Durability.NONE);

    assertEquals(1123581321L, bwConfig.getMaxMemory());
    assertEquals(HOURS.toMillis(22), bwConfig.getMaxLatency(MILLISECONDS));
    assertEquals(DAYS.toMillis(33), bwConfig.getTimeout(MILLISECONDS));
    assertEquals(42, bwConfig.getMaxWriteThreads());
    assertEquals(Durability.NONE, bwConfig.getDurability());
  }

  @Test
  public void testZeroValues() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    bwConfig.setMaxLatency(0, MILLISECONDS);
    bwConfig.setTimeout(0, MILLISECONDS);
    bwConfig.setMaxMemory(0);

    assertEquals(Long.MAX_VALUE, bwConfig.getMaxLatency(MILLISECONDS));
    assertEquals(Long.MAX_VALUE, bwConfig.getTimeout(MILLISECONDS));
    assertEquals(0, bwConfig.getMaxMemory());
  }

  @Test
  public void testNegativeMaxMemory() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    assertThrows(IllegalArgumentException.class, () -> bwConfig.setMaxMemory(-1));
  }

  @Test
  public void testNegativeMaxLatency() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    assertThrows(IllegalArgumentException.class, () -> bwConfig.setMaxLatency(-1, DAYS));
  }

  @Test
  public void testTinyTimeConversions() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    bwConfig.setMaxLatency(999, MICROSECONDS);
    bwConfig.setTimeout(999, MICROSECONDS);

    assertEquals(1000, bwConfig.getMaxLatency(MICROSECONDS));
    assertEquals(1000, bwConfig.getTimeout(MICROSECONDS));
    assertEquals(1, bwConfig.getMaxLatency(MILLISECONDS));
    assertEquals(1, bwConfig.getTimeout(MILLISECONDS));

    bwConfig.setMaxLatency(10, NANOSECONDS);
    bwConfig.setTimeout(10, NANOSECONDS);

    assertEquals(1000000, bwConfig.getMaxLatency(NANOSECONDS));
    assertEquals(1000000, bwConfig.getTimeout(NANOSECONDS));
    assertEquals(1, bwConfig.getMaxLatency(MILLISECONDS));
    assertEquals(1, bwConfig.getTimeout(MILLISECONDS));

  }

  @Test
  public void testNegativeTimeout() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    assertThrows(IllegalArgumentException.class, () -> bwConfig.setTimeout(-1, DAYS));
  }

  @Test
  public void testZeroMaxWriteThreads() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    assertThrows(IllegalArgumentException.class, () -> bwConfig.setMaxWriteThreads(0));
  }

  @Test
  public void testNegativeMaxWriteThreads() {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    assertThrows(IllegalArgumentException.class, () -> bwConfig.setMaxWriteThreads(-1));
  }

  @Test
  public void testSerialize() throws IOException {
    // make sure we aren't testing defaults
    final BatchWriterConfig bwDefaults = new BatchWriterConfig();
    assertNotEquals(7654321L, bwDefaults.getMaxLatency(MILLISECONDS));
    assertNotEquals(9898989L, bwDefaults.getTimeout(MILLISECONDS));
    assertNotEquals(42, bwDefaults.getMaxWriteThreads());
    assertNotEquals(1123581321L, bwDefaults.getMaxMemory());
    assertNotEquals(Durability.FLUSH, bwDefaults.getDurability());

    // test setting all fields
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    bwConfig.setMaxLatency(7654321L, MILLISECONDS);
    bwConfig.setTimeout(9898989L, MILLISECONDS);
    bwConfig.setMaxWriteThreads(42);
    bwConfig.setMaxMemory(1123581321L);
    bwConfig.setDurability(Durability.FLUSH);
    byte[] bytes = createBytes(bwConfig);
    checkBytes(bwConfig, bytes);

    // test human-readable serialization
    bwConfig = new BatchWriterConfig();
    bwConfig.setMaxWriteThreads(42);
    bytes = createBytes(bwConfig);
    assertEquals("     i#maxWriteThreads=42", new String(bytes, UTF_8));
    checkBytes(bwConfig, bytes);

    // test human-readable with 2 fields
    bwConfig = new BatchWriterConfig();
    bwConfig.setMaxWriteThreads(24);
    bwConfig.setTimeout(3, SECONDS);
    bytes = createBytes(bwConfig);
    assertEquals("     v#maxWriteThreads=24,timeout=3000", new String(bytes, UTF_8));
    checkBytes(bwConfig, bytes);

    // test human-readable durability
    bwConfig = new BatchWriterConfig();
    bwConfig.setDurability(Durability.LOG);
    bytes = createBytes(bwConfig);
    assertEquals("     e#durability=LOG", new String(bytes, UTF_8));
  }

  @Test
  public void testDefaultEquality() {
    BatchWriterConfig cfg1 = new BatchWriterConfig(), cfg2 = new BatchWriterConfig();
    assertEquals(cfg1, cfg2);
    assertEquals(cfg1.hashCode(), cfg2.hashCode());
    cfg2.setMaxMemory(1);
    assertNotEquals(cfg1, cfg2);
    cfg2 = new BatchWriterConfig();
    cfg2.setDurability(Durability.FLUSH);
    assertNotEquals(cfg1, cfg2);
    assertNotEquals(cfg1.hashCode(), cfg2.hashCode());
  }

  @Test
  public void testManualEquality() {
    BatchWriterConfig cfg1 = new BatchWriterConfig(), cfg2 = new BatchWriterConfig();
    cfg1.setMaxLatency(10, SECONDS);
    cfg2.setMaxLatency(10000, MILLISECONDS);

    cfg1.setMaxMemory(100);
    cfg2.setMaxMemory(100);

    cfg1.setTimeout(10, SECONDS);
    cfg2.setTimeout(10000, MILLISECONDS);

    assertEquals(cfg1, cfg2);

    assertEquals(cfg1.hashCode(), cfg2.hashCode());
  }

  @Test
  public void testMerge() {
    BatchWriterConfig cfg1 = new BatchWriterConfig(), cfg2 = new BatchWriterConfig();
    cfg1.setMaxMemory(1234);
    cfg2.setMaxMemory(5858);
    cfg2.setDurability(Durability.LOG);
    cfg2.setMaxLatency(456, MILLISECONDS);

    assertEquals(Durability.DEFAULT, cfg1.getDurability());

    BatchWriterConfig merged = cfg1.merge(cfg2);

    assertEquals(1234, merged.getMaxMemory());
    assertEquals(Durability.LOG, merged.getDurability());
    assertEquals(456, merged.getMaxLatency(MILLISECONDS));
    assertEquals(3, merged.getMaxWriteThreads());
  }

  private byte[] createBytes(BatchWriterConfig bwConfig) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    bwConfig.write(new DataOutputStream(baos));
    return baos.toByteArray();
  }

  private void checkBytes(BatchWriterConfig bwConfig, byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    BatchWriterConfig createdConfig = new BatchWriterConfig();
    createdConfig.readFields(new DataInputStream(bais));

    assertEquals(bwConfig.getMaxMemory(), createdConfig.getMaxMemory());
    assertEquals(bwConfig.getMaxLatency(MILLISECONDS), createdConfig.getMaxLatency(MILLISECONDS));
    assertEquals(bwConfig.getTimeout(MILLISECONDS), createdConfig.getTimeout(MILLISECONDS));
    assertEquals(bwConfig.getMaxWriteThreads(), createdConfig.getMaxWriteThreads());
  }

  @Test
  public void countClientProps() {
    // count the number in case one gets added to in one place but not the other
    ClientProperty[] bwProps = Arrays.stream(ClientProperty.values())
        .filter(c -> c.name().startsWith("BATCH_WRITER")).toArray(ClientProperty[]::new);
    assertEquals(5, bwProps.length);
  }

}
