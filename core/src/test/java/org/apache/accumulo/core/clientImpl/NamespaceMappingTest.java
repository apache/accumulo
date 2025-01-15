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
package org.apache.accumulo.core.clientImpl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonSyntaxException;

class NamespaceMappingTest {

  @Test
  void testSerialize() {
    assertThrows(NullPointerException.class, () -> NamespaceMapping.serialize(null));
    assertEquals("{}", new String(NamespaceMapping.serialize(Map.of()), UTF_8));
    assertEquals("{\"a\":\"b\"}", new String(NamespaceMapping.serialize(Map.of("a", "b")), UTF_8));
    assertEquals("{\"a\":\"b\",\"c\":\"d\"}",
        new String(NamespaceMapping.serialize(Map.of("a", "b", "c", "d")), UTF_8));
  }

  @Test
  void testDeserialize() {
    assertThrows(NullPointerException.class, () -> NamespaceMapping.deserialize(null));
    assertEquals(null, NamespaceMapping.deserialize(new byte[0]));
    assertTrue(NamespaceMapping.deserialize("{}".getBytes(UTF_8)) instanceof TreeMap);
    assertEquals(Map.of(), NamespaceMapping.deserialize("{}".getBytes(UTF_8)));
    assertEquals(Map.of("a", "b"), NamespaceMapping.deserialize("{\"a\":\"b\"}".getBytes(UTF_8)));
    assertEquals(Map.of("a", "b", "c", "d"),
        NamespaceMapping.deserialize("{\"a\":\"b\",\"c\":\"d\"}".getBytes(UTF_8)));

    // check malformed json
    assertThrows(JsonSyntaxException.class,
        () -> NamespaceMapping.deserialize("-".getBytes(UTF_8)));
    // check incorrect json type for string value
    assertThrows(JsonSyntaxException.class,
        () -> NamespaceMapping.deserialize("{\"a\":{}}".getBytes(UTF_8)));
    // check valid json, but not a map
    assertThrows(JsonSyntaxException.class,
        () -> NamespaceMapping.deserialize("\"[\"a\"]\"".getBytes(UTF_8)));

    // strange edge case because empty json array can be converted into an empty map; we don't ever
    // expect this to be found, but there's no easy way to check for this edge case that is worth
    // the effort
    assertTrue(NamespaceMapping.deserialize("[]".getBytes(UTF_8)) instanceof SortedMap);
    assertEquals(Map.of(), NamespaceMapping.deserialize("[]".getBytes(UTF_8)));
  }

}
