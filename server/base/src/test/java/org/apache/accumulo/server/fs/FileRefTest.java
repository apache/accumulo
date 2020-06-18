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
package org.apache.accumulo.server.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class FileRefTest {

  private void testBadTableSuffix(String badPath) {
    try {
      FileRef.extractSuffix(new Path(badPath));
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(badPath));
    }
  }

  @Test
  public void testSuffixes() {
    assertEquals(new Path("2a/t-0003/C0004.rf"),
        FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf")));
    assertEquals(new Path("2a/t-0003/C0004.rf"),
        FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003//C0004.rf")));
    assertEquals(new Path("2a/t-0003"),
        FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003")));
    assertEquals(new Path("2a/t-0003"),
        FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003/")));

    testBadTableSuffix("t-0003/C0004.rf");
    testBadTableSuffix("../t-0003/C0004.rf");
    testBadTableSuffix("2a/t-0003");
    testBadTableSuffix("2a/t-0003/C0004.rf");
    testBadTableSuffix("2a/t-0003/C0004.rf");
    testBadTableSuffix("hdfs://nn3/accumulo/2a/t-0003/C0004.rf");
    testBadTableSuffix("hdfs://nn3/accumulo/2a/t-0003");
    testBadTableSuffix("hdfs://nn3/tables/accumulo/2a/t-0003/C0004.rf");
  }

  @Test
  public void testEqualsAndHash() {
    assertEquals(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"),
        new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));
    assertEquals(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"),
        new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));
    assertNotEquals(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf"),
        new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));
    assertNotEquals(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf"),
        new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));

    HashMap<FileRef,String> refMap = new HashMap<>();
    refMap.put(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"), "7");
    refMap.put(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf"), "8");

    assertNull(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf")));

    assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")), "7");
    assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf")), "7");
    assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4//accumulo/tables/2a//t-0003//C0004.rf")),
        "7");
    assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables/2a//t-0003//C0004.rf")), "7");

    assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf")), "8");
    assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf")), "8");
    assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a//t-0003/C0005.rf")),
        "8");
    assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003/C0005.rf")), "8");
  }

  @Test
  public void testCompareTo() {
    assertEquals(0, new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")));
    assertEquals(0, new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf")));
    assertEquals(0, new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003//C0004.rf")));

    assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf")) < 0);
    assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf")) < 0);
    assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003//C0005.rf")) < 0);

    assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf")
        .compareTo(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf")) > 0);
    assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf")) > 0);
    assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003//C0005.rf")) > 0);

  }
}
