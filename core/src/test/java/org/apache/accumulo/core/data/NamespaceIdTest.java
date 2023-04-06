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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.WithTestNames;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the NamespaceId class, mainly the internal cache.
 */
public class NamespaceIdTest extends WithTestNames {

  private static final Logger LOG = LoggerFactory.getLogger(NamespaceIdTest.class);

  private static long cacheCount() {
    // guava cache size() is approximate, and can include garbage-collected entries
    // so we iterate to get the actual cache size
    return NamespaceId.cache.asMap().entrySet().stream().count();
  }

  @Test
  public void testCacheNoDuplicates() {
    // the next line just preloads the built-ins, since they now exist in a separate class from
    // NamespaceId, and aren't preloaded when the NamespaceId class is referenced
    assertNotSame(Namespace.ACCUMULO.id(), Namespace.DEFAULT.id());

    String namespaceString = "namespace-" + testName();
    long initialSize = cacheCount();
    NamespaceId nsId = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, cacheCount());
    assertEquals(namespaceString, nsId.canonical());

    // ensure duplicates are not created
    NamespaceId builtInNamespaceId = NamespaceId.of("+accumulo");
    assertSame(Namespace.ACCUMULO.id(), builtInNamespaceId);
    builtInNamespaceId = NamespaceId.of("+default");
    assertSame(Namespace.DEFAULT.id(), builtInNamespaceId);
    nsId = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, cacheCount());
    assertEquals(namespaceString, nsId.canonical());
    NamespaceId nsId2 = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, cacheCount());
    assertSame(nsId, nsId2);
  }

  @Test
  @Timeout(30)
  public void testCacheIncreasesAndDecreasesAfterGC() {
    long initialSize = cacheCount();
    assertTrue(initialSize < 20); // verify initial amount is reasonably low
    LOG.info("Initial cache size: {}", initialSize);
    LOG.info(NamespaceId.cache.asMap().toString());

    // add one and check increase
    String namespaceString = "namespace-" + testName();
    NamespaceId nsId = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, cacheCount());
    assertEquals(namespaceString, nsId.canonical());

    // create a bunch more and throw them away
    long preGCSize = 0;
    int i = 0;
    while ((preGCSize = cacheCount()) < 100) {
      NamespaceId.of(new String("namespace" + i++));
    }
    LOG.info("Entries before System.gc(): {}", preGCSize);
    assertEquals(100, preGCSize);
    long postGCSize = preGCSize;
    while (postGCSize >= preGCSize) {
      TableIdTest.tryToGc();
      postGCSize = cacheCount();
      LOG.info("Entries after System.gc(): {}", postGCSize);
    }
  }
}
