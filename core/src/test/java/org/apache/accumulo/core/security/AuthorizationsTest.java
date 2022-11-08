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
package org.apache.accumulo.core.security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.util.ByteArraySet;
import org.junit.jupiter.api.Test;

public class AuthorizationsTest {

  @Test
  public void testSetOfByteArrays() {
    assertTrue(ByteArraySet.fromStrings("a", "b", "c").contains("a".getBytes()));
  }

  @Test
  public void testEncodeDecode() {
    Authorizations a = new Authorizations("a", "abcdefg", "hijklmno", ",");
    byte[] array = a.getAuthorizationsArray();
    Authorizations b = new Authorizations(array);
    assertEquals(a, b);

    // test encoding empty auths
    a = new Authorizations();
    array = a.getAuthorizationsArray();
    b = new Authorizations(array);
    assertEquals(a, b);

    // test encoding multi-byte auths
    a = new Authorizations("五", "b", "c", "九");
    array = a.getAuthorizationsArray();
    b = new Authorizations(array);
    assertEquals(a, b);
  }

  @Test
  public void testSerialization() {
    Authorizations a1 = new Authorizations("a", "b");
    Authorizations a2 = new Authorizations("b", "a");

    assertEquals(a1, a2);
    assertEquals(a1.serialize(), a2.serialize());
  }

  @Test
  public void testDefensiveAccess() {
    Authorizations expected = new Authorizations("foo", "a");
    Authorizations actual = new Authorizations("foo", "a");

    // foo to goo; test defensive iterator
    for (byte[] bytes : actual) {
      bytes[0]++;
    }
    assertArrayEquals(expected.getAuthorizationsArray(), actual.getAuthorizationsArray());

    // test defensive getter and serializer
    actual.getAuthorizations().get(0)[0]++;
    assertArrayEquals(expected.getAuthorizationsArray(), actual.getAuthorizationsArray());
    assertEquals(expected.serialize(), actual.serialize());
  }

  // This should throw ReadOnlyBufferException, but THRIFT-883 requires that the ByteBuffers
  // themselves not be read-only
  // @Test(expected = ReadOnlyBufferException.class)
  @Test
  public void testReadOnlyByteBuffer() {
    Authorizations expected = new Authorizations("foo");
    Authorizations actual = new Authorizations("foo");

    assertArrayEquals(expected.getAuthorizationsArray(), actual.getAuthorizationsArray());
    actual.getAuthorizationsBB().get(0).array()[0]++;
    assertArrayEquals(expected.getAuthorizationsArray(), actual.getAuthorizationsArray());
  }

  @Test
  public void testUnmodifiableList() {
    Authorizations expected = new Authorizations("foo");
    Authorizations actual = new Authorizations("foo");

    assertArrayEquals(expected.getAuthorizationsArray(), actual.getAuthorizationsArray());
    assertThrows(UnsupportedOperationException.class,
        () -> actual.getAuthorizationsBB().add(ByteBuffer.wrap(new byte[] {'a'})));
  }
}
