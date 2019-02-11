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
package org.apache.accumulo.core.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests the NamespaceId class, mainly the internal cache.
 */
public class NamespaceIdTest {
  @Rule
  public TestName name = new TestName();

  @Test
  public void testCacheIncreases() {
    String namespaceString = "namespace-" + name.getMethodName();
    Long initialSize = NamespaceId.cache.asMap().entrySet().stream().count();
    NamespaceId nsId = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, NamespaceId.cache.asMap().entrySet().stream().count());
    assertEquals(namespaceString, nsId.canonical());
  }

  @Test
  public void testCacheNoDuplicates() {
    // the next line just preloads the built-ins, since they now exist in a separate class from
    // NamespaceId, and aren't preloaded when the NamespaceId class is referenced
    assertNotSame(Namespace.ACCUMULO.id(), Namespace.DEFAULT.id());

    String namespaceString = "namespace-" + name.getMethodName();
    Long initialSize = NamespaceId.cache.asMap().entrySet().stream().count();
    NamespaceId nsId = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, NamespaceId.cache.asMap().entrySet().stream().count());
    assertEquals(namespaceString, nsId.canonical());

    // ensure duplicates are not created
    NamespaceId builtInNamespaceId = NamespaceId.of("+accumulo");
    assertSame(Namespace.ACCUMULO.id(), builtInNamespaceId);
    builtInNamespaceId = NamespaceId.of("+default");
    assertSame(Namespace.DEFAULT.id(), builtInNamespaceId);
    nsId = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, NamespaceId.cache.asMap().entrySet().stream().count());
    assertEquals(namespaceString, nsId.canonical());
    NamespaceId nsId2 = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, NamespaceId.cache.asMap().entrySet().stream().count());
    assertSame(nsId, nsId2);
  }

  @Test(timeout = 60_000)
  public void testCacheDecreasesAfterGC() {
    Long initialSize = NamespaceId.cache.asMap().entrySet().stream().count();
    generateJunkCacheEntries();
    Long postGCSize;
    do {
      System.gc();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        fail("Thread interrupted while waiting for GC");
      }
      postGCSize = NamespaceId.cache.asMap().entrySet().stream().count();
    } while (postGCSize > initialSize);

    assertTrue("Cache did not decrease with GC.",
        NamespaceId.cache.asMap().entrySet().stream().count() < initialSize);
  }

  private void generateJunkCacheEntries() {
    for (int i = 0; i < 1000; i++)
      NamespaceId.of(new String("namespace" + i));
  }
}
