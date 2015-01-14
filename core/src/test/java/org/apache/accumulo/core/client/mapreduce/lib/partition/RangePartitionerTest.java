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
package org.apache.accumulo.core.client.mapreduce.lib.partition;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

public class RangePartitionerTest {

  private static Text[] cutArray = new Text[] {new Text("A"), new Text("B"), new Text("C")};

  @Test
  public void testNoSubBins() throws IOException {
    for (int i = -2; i < 2; ++i) {
      checkExpectedBins(i, new String[] {"A", "B", "C"}, new int[] {0, 1, 2});
      checkExpectedBins(i, new String[] {"C", "A", "B"}, new int[] {2, 0, 1});
      checkExpectedBins(i, new String[] {"", "AA", "BB", "CC"}, new int[] {0, 1, 2, 3});
    }
  }

  @Test
  public void testSubBins() throws IOException {
    checkExpectedRangeBins(2, new String[] {"A", "B", "C"}, new int[] {1, 3, 5});
    checkExpectedRangeBins(2, new String[] {"C", "A", "B"}, new int[] {5, 1, 3});
    checkExpectedRangeBins(2, new String[] {"", "AA", "BB", "CC"}, new int[] {1, 3, 5, 7});

    checkExpectedRangeBins(3, new String[] {"A", "B", "C"}, new int[] {2, 5, 8});
    checkExpectedRangeBins(3, new String[] {"C", "A", "B"}, new int[] {8, 2, 5});
    checkExpectedRangeBins(3, new String[] {"", "AA", "BB", "CC"}, new int[] {2, 5, 8, 11});

    checkExpectedRangeBins(10, new String[] {"A", "B", "C"}, new int[] {9, 19, 29});
    checkExpectedRangeBins(10, new String[] {"C", "A", "B"}, new int[] {29, 9, 19});
    checkExpectedRangeBins(10, new String[] {"", "AA", "BB", "CC"}, new int[] {9, 19, 29, 39});
  }

  private RangePartitioner prepPartitioner(int numSubBins) throws IOException {
    Job job = Job.getInstance();
    RangePartitioner.setNumSubBins(job, numSubBins);
    RangePartitioner rp = new RangePartitioner();
    rp.setConf(job.getConfiguration());
    return rp;
  }

  private void checkExpectedRangeBins(int numSubBins, String[] strings, int[] rangeEnds) throws IOException {
    assertTrue(strings.length == rangeEnds.length);
    for (int i = 0; i < strings.length; ++i) {
      int endRange = rangeEnds[i];
      int startRange = endRange + 1 - numSubBins;
      int part = prepPartitioner(numSubBins).findPartition(new Text(strings[i]), cutArray, numSubBins);
      assertTrue(part >= startRange);
      assertTrue(part <= endRange);
    }
  }

  private void checkExpectedBins(int numSubBins, String[] strings, int[] bins) throws IOException {
    assertTrue(strings.length == bins.length);
    for (int i = 0; i < strings.length; ++i) {
      int bin = bins[i], part = prepPartitioner(numSubBins).findPartition(new Text(strings[i]), cutArray, numSubBins);
      assertTrue(bin == part);
    }
  }
}
