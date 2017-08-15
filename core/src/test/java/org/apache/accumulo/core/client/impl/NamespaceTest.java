/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.impl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the Namespace ID class, mainly the internal WeakHashMap.
 */
public class NamespaceTest {

  @Test
  public void testWeakHashMapIncreases() {
    String namespaceString = "namespace-testWeakHashMapIncreases";
    Integer initialSize = Namespace.ID.cache.asMap().size();
    Namespace.ID nsId = Namespace.ID.of(namespaceString);
    assertEquals(initialSize + 1, Namespace.ID.cache.asMap().size());
    assertEquals(namespaceString, nsId.canonicalID());
  }

  @Test
  public void testWeakHashMapNoDuplicates() {
    String namespaceString = "namespace-testWeakHashMapNoDuplicates";
    Integer initialSize = Namespace.ID.cache.asMap().size();
    Namespace.ID nsId = Namespace.ID.of(namespaceString);
    assertEquals(initialSize + 1, Namespace.ID.cache.asMap().size());
    assertEquals(namespaceString, nsId.canonicalID());

    // ensure duplicates are not created
    Namespace.ID builtInNamespaceId = Namespace.ID.of("+accumulo");
    assertEquals(Namespace.ID.ACCUMULO, builtInNamespaceId);
    builtInNamespaceId = Namespace.ID.of("+default");
    assertEquals(Namespace.ID.DEFAULT, builtInNamespaceId);
    nsId = Namespace.ID.of(namespaceString);
    assertEquals(initialSize + 1, Namespace.ID.cache.asMap().size());
    assertEquals(namespaceString, nsId.canonicalID());
  }
}
