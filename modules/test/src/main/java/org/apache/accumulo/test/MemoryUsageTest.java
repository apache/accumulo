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
package org.apache.accumulo.test;

abstract class MemoryUsageTest {
  abstract void addEntry(int i);

  abstract int getEstimatedBytesPerEntry();

  abstract void clear();

  abstract int getNumPasses();

  abstract String getName();

  abstract void init();

  public void run() {
    System.gc();
    long usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    int count = 0;
    while (usedMem > 1024 * 1024 && count < 10) {
      System.gc();
      usedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
      count++;
    }

    init();

    for (int i = 0; i < getNumPasses(); i++) {
      addEntry(i);
    }

    System.gc();

    long memSize = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) - usedMem;

    double actualBytesPerEntry = memSize / (double) getNumPasses();
    double expectedBytesPerEntry = getEstimatedBytesPerEntry();
    double diff = actualBytesPerEntry - expectedBytesPerEntry;
    double ratio = actualBytesPerEntry / expectedBytesPerEntry * 100;

    System.out.printf("%30s | %,10d | %6.2fGB | %6.2f | %6.2f | %6.2f | %6.2f%s%n", getName(), getNumPasses(), memSize / (1024 * 1024 * 1024.0),
        actualBytesPerEntry, expectedBytesPerEntry, diff, ratio, "%");

    clear();

  }

}
