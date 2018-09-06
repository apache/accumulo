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

package org.apache.accumulo.core.spi.scan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.accumulo.core.spi.scan.ScanInfo.Type;
import org.junit.Assert;
import org.junit.Test;

public class IdleRatioScanPrioritizerTest {

  @Test
  public void testSort() {
    long now = System.currentTimeMillis();

    List<TestScanInfo> scans = new ArrayList<>();

    // Two following have never run, so oldest should go first
    scans.add(new TestScanInfo("a", Type.SINGLE, now - 3));
    scans.add(new TestScanInfo("b", Type.SINGLE, now - 8));
    // Two following have different idle ratio and same last run times
    scans.add(new TestScanInfo("c", Type.SINGLE, now - 16, 2, 10));
    scans.add(new TestScanInfo("d", Type.SINGLE, now - 16, 5, 10));
    // Two following have same idle ratio and different last run times
    scans.add(new TestScanInfo("e", Type.SINGLE, now - 12, 5, 9));
    scans.add(new TestScanInfo("f", Type.SINGLE, now - 12, 3, 7));

    Collections.shuffle(scans);

    Comparator<ScanInfo> comparator = new IdleRatioScanPrioritizer()
        .createComparator(Collections::emptyMap);

    Collections.sort(scans, comparator);

    Assert.assertEquals("b", scans.get(0).testId);
    Assert.assertEquals("a", scans.get(1).testId);
    Assert.assertEquals("f", scans.get(2).testId);
    Assert.assertEquals("e", scans.get(3).testId);
    Assert.assertEquals("d", scans.get(4).testId);
    Assert.assertEquals("c", scans.get(5).testId);
  }
}
