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
package org.apache.accumulo.core.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class ByteSequenceTest {

  @Test
  public void testCompareBytes() {
    ByteSequence a = new ArrayByteSequence("a");
    ByteSequence b = new ArrayByteSequence("b");
    ByteSequence abc = new ArrayByteSequence("abc");

    assertLessThan(a, b);
    assertLessThan(a, abc);
    assertLessThan(abc, b);
  }

  private void assertLessThan(ByteSequence lhs, ByteSequence rhs) {
    int result = ByteSequence.compareBytes(lhs, rhs);
    assertTrue(result < 0);
  }

  @Test
  public void testHashCode() {
    // Testing consistency
    ByteSequence byteSeq = new ArrayByteSequence("value");
    int hashCode1 = byteSeq.hashCode();
    int hashCode2 = byteSeq.hashCode();
    assertEquals(hashCode1, hashCode2);

    // Testing equality
    ByteSequence byteOne = new ArrayByteSequence("value");
    ByteSequence byteTwo = new ArrayByteSequence("value");
    assertEquals(byteOne.hashCode(), byteTwo.hashCode());

    // Testing even distribution
    List<ByteSequence> byteSequences = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      byteSequences.add(new ArrayByteSequence("value" + i));
    }
    Set<Integer> hashCodes = new HashSet<>();
    for (ByteSequence bytes : byteSequences) {
      hashCodes.add(bytes.hashCode());
    }
    assertEquals(byteSequences.size(), hashCodes.size(), 10);
  }

}
