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
package org.apache.accumulo.server.gc;

import org.junit.Assert;
import org.junit.Test;

public class SimpleGarbageCollectorTest {
  @Test
  public void testValidation() {

    Assert.assertTrue(SimpleGarbageCollector.isValidDirDelete("/a/b"));
    Assert.assertTrue(SimpleGarbageCollector.isValidDirDelete("/45/t-00004"));
    Assert.assertTrue(SimpleGarbageCollector.isValidDirDelete("/45/b-00006"));

    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete(""));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("/"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("/a"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("//a"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("//"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("////"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("/45//t-00004"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("45///b-00006"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("/45/b-00006/C00000.rf"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("/ /b"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("/a/b/c"));
    Assert.assertFalse(SimpleGarbageCollector.isValidDirDelete("zz/a/b"));

    Assert.assertTrue(SimpleGarbageCollector.isValidFileDelete("/45/t-000c4/C00000.rf"));
    Assert.assertTrue(SimpleGarbageCollector.isValidFileDelete("/a7/b-00006/F0000a.rf"));

    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete(""));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("/"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("a7/b-00006/F0000a.rf"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("//a7/b-00006/F0000a.rf"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("///"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("//////"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("///F0000a.rf"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("hh/a7/b-00006/F0000a.rf"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("/a7/b-00006/"));
    Assert.assertFalse(SimpleGarbageCollector.isValidFileDelete("/a7/b-00006"));

  }
}
