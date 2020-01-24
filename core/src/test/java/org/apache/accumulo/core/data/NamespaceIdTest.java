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
package org.apache.accumulo.core.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the NamespaceId class, mainly the internal cache.
 */
public class NamespaceIdTest {

  private static final Logger LOG = LoggerFactory.getLogger(NamespaceIdTest.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testCacheNoDuplicates() {
    // the next line just preloads the built-ins, since they now exist in a separate class from
    // NamespaceId, and aren't preloaded when the NamespaceId class is referenced
    assertNotSame(Namespace.ACCUMULO.id(), Namespace.DEFAULT.id());

    String namespaceString = "namespace-" + name.getMethodName();
    long initialSize = NamespaceId.cache.asMap().entrySet().stream().count();
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

  @Test(timeout = 30_000)
  public void testCacheIncreasesAndDecreasesAfterGC() {
    long initialSize = NamespaceId.cache.asMap().entrySet().stream().count();
    assertTrue(initialSize < 20); // verify initial amount is reasonably low
    LOG.info("Initial cache size: {}", initialSize);
    LOG.info(NamespaceId.cache.asMap().toString());

    // add one and check increase
    String namespaceString = "namespace-" + name.getMethodName();
    NamespaceId nsId = NamespaceId.of(namespaceString);
    assertEquals(initialSize + 1, NamespaceId.cache.asMap().entrySet().stream().count());
    assertEquals(namespaceString, nsId.canonical());

    // create a bunch more and throw them away
    for (int i = 0; i < 999; i++) {
      NamespaceId.of(new String("namespace" + i));
    }
    long preGCSize = NamespaceId.cache.asMap().entrySet().stream().count();
    LOG.info("Entries before System.gc(): {}", preGCSize);
    assertTrue(preGCSize > 500); // verify amount increased significantly
    long postGCSize = preGCSize;
    while (postGCSize >= preGCSize) {
      TableIdTest.tryToGc();
      postGCSize = NamespaceId.cache.asMap().entrySet().stream().count();
      LOG.info("Entries after System.gc(): {}", postGCSize);
    }
  }
}
