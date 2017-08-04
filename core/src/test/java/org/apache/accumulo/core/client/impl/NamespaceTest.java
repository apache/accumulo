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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Test;

/**
 * Tests the Namespace ID class, mainly the internal WeakHashMap.
 */
public class NamespaceTest {

  @After
  public void cleanup() {
    garbageCollect(Namespace.ID.namespaceIds.size());
  }

  @Test
  public void testWeakHashMapIncreases() {
    // ensure initial size contains just the built in Namespace IDs (accumulo, default)
    assertEquals(2, Namespace.ID.namespaceIds.size());

    String namespaceString = new String("namespace-testWeakHashMapIncreases");
    Namespace.ID nsId = Namespace.ID.of(namespaceString);
    assertEquals(3, Namespace.ID.namespaceIds.size());
    assertEquals(namespaceString, nsId.canonicalID());
  }

  @Test
  public void testWeakHashMapNoDuplicates() {
    String namespaceString = new String("namespace-testWeakHashMapNoDuplicates");
    Namespace.ID nsId = Namespace.ID.of(namespaceString);
    assertEquals(3, Namespace.ID.namespaceIds.size());
    assertEquals(namespaceString, nsId.canonicalID());

    // ensure duplicates are not created
    Namespace.ID builtInNamespaceId = Namespace.ID.of("+accumulo");
    assertEquals(Namespace.ID.ACCUMULO, builtInNamespaceId);
    builtInNamespaceId = Namespace.ID.of("+default");
    assertEquals(Namespace.ID.DEFAULT, builtInNamespaceId);
    nsId = Namespace.ID.of(namespaceString);
    assertEquals(3, Namespace.ID.namespaceIds.size());
    assertEquals(namespaceString, nsId.canonicalID());
  }

  @Test
  public void testWeakHashMapDecreases() {
    // get bunch of namespace IDs that are not used
    for (int i = 0; i < 1000; i++)
      Namespace.ID.of(new String("namespace" + i));
  }

  private void garbageCollect(Integer initialSize) {
    System.out.println("GC with initial Size = " + initialSize);
    int count = 0;
    while (Namespace.ID.namespaceIds.size() >= initialSize && count < 10) {
      System.gc();
      count++;
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        fail("Thread interrupted while waiting for GC");
        break;
      }
      System.out.println("after GC: Size of namespaceIds = " + Namespace.ID.namespaceIds.size());
    }
    assertTrue("Size of WeakHashMap did not decrease after GC.", Namespace.ID.namespaceIds.size() < initialSize);
  }

}
