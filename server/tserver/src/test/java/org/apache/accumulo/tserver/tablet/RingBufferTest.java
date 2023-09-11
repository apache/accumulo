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
package org.apache.accumulo.tserver.tablet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;

import org.junit.jupiter.api.Test;

public class RingBufferTest {

  @Test
  public void goPathTest() {
    DatafileTransactionLog.Ring<String> ring = new DatafileTransactionLog.Ring<>(4);
    ring.add("1");
    ring.add("2");
    ring.add("3");
    ring.add("4");

    assertFalse(ring.isEmpty());
    assertEquals(4, ring.capacity());
    assertEquals(List.of("1", "2", "3", "4"), ring.toList());
  }

  @Test
  public void overflowTest() {
    DatafileTransactionLog.Ring<String> ring = new DatafileTransactionLog.Ring<>(4);
    ring.add("1");
    ring.add("2");
    ring.add("3");
    ring.add("4");
    assertEquals("1", ring.add("5"));

    assertFalse(ring.isEmpty());
    assertEquals(4, ring.capacity());
    assertEquals(List.of("2", "3", "4", "5"), ring.toList());
  }

  @Test
  public void oddCapacityTest() {
    DatafileTransactionLog.Ring<String> ring = new DatafileTransactionLog.Ring<>(3);
    ring.add("1");
    ring.add("2");
    ring.add("3");
    assertEquals("1", ring.add("4"));
    assertEquals("2", ring.add("5"));

    assertFalse(ring.isEmpty());
    assertEquals(3, ring.capacity());
    assertEquals(List.of("3", "4", "5"), ring.toList());
  }

  @Test
  public void zeroLengthTest() {
    DatafileTransactionLog.Ring<String> ring = new DatafileTransactionLog.Ring<>(0);
    assertEquals("1", ring.add("1"));
  }
}
