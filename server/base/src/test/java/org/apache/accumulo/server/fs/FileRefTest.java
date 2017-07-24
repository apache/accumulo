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
package org.apache.accumulo.server.fs;

import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class FileRefTest {

  private void testBadTableSuffix(String badPath) {
    try {
      FileRef.extractSuffix(new Path(badPath));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(badPath));
    }
  }

  @Test
  public void testSuffixes() {
    Assert.assertEquals(new Path("2a/t-0003/C0004.rf"), FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf")));
    Assert.assertEquals(new Path("2a/t-0003/C0004.rf"), FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003//C0004.rf")));
    Assert.assertEquals(new Path("2a/t-0003"), FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003")));
    Assert.assertEquals(new Path("2a/t-0003"), FileRef.extractSuffix(new Path("hdfs://nn1/accumulo/tables/2a/t-0003/")));

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
    Assert.assertEquals(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"), new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));
    Assert.assertEquals(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"), new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));
    Assert.assertNotEquals(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf"), new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));
    Assert.assertNotEquals(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf"), new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf"));

    HashMap<FileRef,String> refMap = new HashMap<>();
    refMap.put(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"), "7");
    refMap.put(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf"), "8");

    Assert.assertNull(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf")));

    Assert.assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")), "7");
    Assert.assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf")), "7");
    Assert.assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4//accumulo/tables/2a//t-0003//C0004.rf")), "7");
    Assert.assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables/2a//t-0003//C0004.rf")), "7");

    Assert.assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf")), "8");
    Assert.assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf")), "8");
    Assert.assertEquals(refMap.get(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a//t-0003/C0005.rf")), "8");
    Assert.assertEquals(refMap.get(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003/C0005.rf")), "8");
  }

  @Test
  public void testCompareTo() {
    Assert.assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf").compareTo(new FileRef(
        "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")) == 0);
    Assert
        .assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf").compareTo(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0004.rf")) == 0);
    Assert.assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003//C0004.rf")) == 0);

    Assert.assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf").compareTo(new FileRef(
        "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf")) < 0);
    Assert
        .assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf").compareTo(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf")) < 0);
    Assert.assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003//C0005.rf")) < 0);

    Assert.assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf").compareTo(new FileRef(
        "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0005.rf")) > 0);
    Assert
        .assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf").compareTo(new FileRef("hdfs://nn1/accumulo/tables/2a/t-0003/C0005.rf")) > 0);
    Assert.assertTrue(new FileRef("hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0006.rf")
        .compareTo(new FileRef("hdfs://nn1/accumulo/tables//2a/t-0003//C0005.rf")) > 0);

  }
}
