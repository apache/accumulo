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
package org.apache.accumulo.test.util.memory;

public class EstimateInMemMapOverhead {

  private static void check(int numEntries, int keyLen, int colFamLen, int colQualLen,
      int colVisLen, int dataLen) {
    new IntObjectMemoryUsageCheck(numEntries).run();
    new InMemoryMapMemoryUsageCheck(numEntries, keyLen, colFamLen, colQualLen, colVisLen, dataLen)
        .run();
    new TextMemoryUsageCheck(numEntries, keyLen, colFamLen, colQualLen, dataLen).run();
    new MutationMemoryUsageCheck(numEntries, keyLen, colFamLen, colQualLen, dataLen).run();
  }

  public static void main(String[] args) {
    check(10000, 10, 4, 4, 4, 20);
    check(100000, 10, 4, 4, 4, 20);
    check(500000, 10, 4, 4, 4, 20);
    check(1000000, 10, 4, 4, 4, 20);
    check(2000000, 10, 4, 4, 4, 20);

    check(10000, 20, 5, 5, 5, 500);
    check(100000, 20, 5, 5, 5, 500);
    check(500000, 20, 5, 5, 5, 500);
    check(1000000, 20, 5, 5, 5, 500);
    check(2000000, 20, 5, 5, 5, 500);

    check(10000, 40, 10, 10, 10, 1000);
    check(100000, 40, 10, 10, 10, 1000);
    check(500000, 40, 10, 10, 10, 1000);
    check(1000000, 40, 10, 10, 10, 1000);
    check(2000000, 40, 10, 10, 10, 1000);
  }

}
