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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Tests the Namespace ID class, mainly the internal cache.
 */
public class NamespaceTest {
  @Rule
  public TestName name = new TestName();

  @Test
  public void testCacheIncreases() {
    String namespaceString = "namespace-" + name.getMethodName();
    Long initialSize = Namespace.ID.cache.asMap().entrySet().stream().count();
    Namespace.ID nsId = Namespace.ID.of(namespaceString);
    assertEquals(initialSize + 1, Namespace.ID.cache.asMap().entrySet().stream().count());
    assertEquals(namespaceString, nsId.canonicalID());
  }

  @Test
  public void testCacheNoDuplicates() {
    String namespaceString = "namespace-" + name.getMethodName();
    Long initialSize = Namespace.ID.cache.asMap().entrySet().stream().count();
    Namespace.ID nsId = Namespace.ID.of(namespaceString);
    assertEquals(initialSize + 1, Namespace.ID.cache.asMap().entrySet().stream().count());
    assertEquals(namespaceString, nsId.canonicalID());

    // ensure duplicates are not created
    Namespace.ID builtInNamespaceId = Namespace.ID.of("+accumulo");
    assertSame(Namespace.ID.ACCUMULO, builtInNamespaceId);
    builtInNamespaceId = Namespace.ID.of("+default");
    assertSame(Namespace.ID.DEFAULT, builtInNamespaceId);
    nsId = Namespace.ID.of(namespaceString);
    assertEquals(initialSize + 1, Namespace.ID.cache.asMap().entrySet().stream().count());
    assertEquals(namespaceString, nsId.canonicalID());
    Namespace.ID nsId2 = Namespace.ID.of(namespaceString);
    assertEquals(initialSize + 1, Namespace.ID.cache.asMap().entrySet().stream().count());
    assertSame(nsId, nsId2);
  }

  @Test(timeout = 60_000)
  public void testCacheDecreasesAfterGC() {
    Long initialSize = Namespace.ID.cache.asMap().entrySet().stream().count();
    generateJunkCacheEntries();
    Long postGCSize;
    do {
      System.gc();
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        fail("Thread interrupted while waiting for GC");
      }
      postGCSize = Namespace.ID.cache.asMap().entrySet().stream().count();
    } while (postGCSize > initialSize);

    assertTrue("Cache did not decrease with GC.", Namespace.ID.cache.asMap().entrySet().stream().count() < initialSize);
  }

  private void generateJunkCacheEntries() {
    for (int i = 0; i < 1000; i++)
      Namespace.ID.of(new String("namespace" + i));
  }
}
