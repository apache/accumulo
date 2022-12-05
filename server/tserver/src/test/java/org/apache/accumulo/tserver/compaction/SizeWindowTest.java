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
package org.apache.accumulo.tserver.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.tserver.compaction.DefaultCompactionStrategy.SizeWindow;
import org.junit.jupiter.api.Test;

public class SizeWindowTest {

  static class TestSizeWindow extends SizeWindow {
    private static Map<StoredTabletFile,DataFileValue> convert(Map<String,Integer> testData) {
      Map<StoredTabletFile,DataFileValue> files = new HashMap<>();
      testData.forEach((k, v) -> {
        files.put(new StoredTabletFile("hdfs://nn1/accumulo/tables/5/t-0001/" + k),
            new DataFileValue(v, 0));
      });
      return files;
    }

    /**
     * A constructor that is more friendly for testing.
     */
    TestSizeWindow(Map<String,Integer> testData) {
      super(convert(testData));
    }
  }

  static Collection<String> getFileNames(SizeWindow sw) {
    return sw.getFiles().stream().map(TabletFile::getFileName).collect(Collectors.toSet());
  }

  static Map<String,Integer> genTestData(int start, int end) {
    Map<String,Integer> testData = new HashMap<>();

    for (int i = start; i <= end; i++) {
      testData.put("F" + i, i);
    }

    return testData;
  }

  @Test
  public void testSlide() {

    Map<String,Integer> testData = genTestData(1, 20);

    TestSizeWindow tsw = new TestSizeWindow(testData);

    SizeWindow tail = tsw.tail(10);

    for (int i = 10; i <= 20; i++) {
      int expectedSum = i * (i + 1) / 2 - ((i - 10) * (i - 9) / 2);
      assertEquals(expectedSum, tail.sum());
      assertEquals(10, tail.size());
      assertEquals(genTestData(i - 9, i).keySet(), getFileNames(tail));
      assertEquals(i, tail.topSize());
      if (tail.slideUp()) {
        assertTrue(i < 20);
      } else {
        assertEquals(20, i);
      }
    }
  }

  @Test
  public void testPop() {
    Map<String,Integer> testData = genTestData(1, 20);

    TestSizeWindow tsw = new TestSizeWindow(testData);

    SizeWindow tail = tsw.tail(10);

    int expectedSize = 10;

    while (expectedSize > 0) {
      int expectedSum = expectedSize * (expectedSize + 1) / 2;
      assertEquals(expectedSum, tail.sum());
      assertEquals(expectedSize, tail.size());
      assertEquals(genTestData(1, expectedSize).keySet(), getFileNames(tail));
      assertEquals(expectedSize, tail.topSize());

      tail.pop();
      expectedSize--;
    }
  }
}
