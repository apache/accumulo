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
package org.apache.accumulo.core.file.rfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.crypto.CryptoTest;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RangedRFileTest extends RFileTest {

  @BeforeAll
  public static void setupCryptoKeyFile() throws Exception {
    CryptoTest.setupKeyFiles(RangedRFileTest.class);
  }

  public static class RangedTestRfile extends TestRFile {
    final List<Range> ranges;

    public RangedTestRfile(AccumuloConfiguration accumuloConfiguration, List<Range> ranges) {
      super(accumuloConfiguration);
      this.ranges = ranges;
    }

    @Override
    protected RFile.RFileReader newReader(CachableBlockFile.CachableBuilder cb) throws IOException {
      return new RFile.RangedReader(cb, ranges);
    }
  }

  @Test
  public void testFencingNoRange() throws IOException {
    // Test with infinite start/end range
    // Expect entire range to be seen
    assertEquals(1024, testFencing(List.of(new Range()), List.of(new Range())));
  }

  @Test
  public void testFencing1() throws IOException {
    // Test with fenced starting range at beginning and infinite end range
    // Expect entire range to be seen
    assertEquals(1024, testFencing(List.of(new Range("r_000000", null)), List.of(new Range())));
  }

  @Test
  public void testFencing2() throws IOException {
    // Test with 2 ranges that are continuous which should be merged
    // Expect entire range to be seen
    assertEquals(1024, testFencing(
        List.of(new Range(null, "r_000002"), new Range("r_000002", null)), List.of(new Range())));
  }

  @Test
  public void testFencing3() throws IOException {
    // Create a fence that contains only row 0 row 2
    // Expect only to see values from those two rows and not row 1 or row 3
    final List<Range> ranges = List.of(new Range(null, true, "r_000001", false),
        new Range("r_000002", true, "r_000003", false));

    // Use the same range for the fence and testing to make sure only the expected keys were seen
    // Should only be 512 keys as 2 rows * 256
    assertEquals(512, testFencing(ranges, ranges));
  }

  // Should fail
  @Test
  public void testFencing4() throws IOException {
    // Create a fence that contains row 0 and row 2 only
    final List<Range> ranges = List.of(new Range(null, true, "r_000001", false),
        new Range("r_000002", true, "r_000003", false));

    // Expected range contains only row 2 so should fail as row 1 should also be seen
    final List<Range> ranges2 = List.of(new Range(null, true, "r_000002", false));

    boolean failed = false;
    try {
      testFencing(ranges, ranges2);
    } catch (AssertionError e) {
      // expected
      failed = true;
    }

    assertTrue(failed, "should have failed");
  }

  @Test
  public void testFencing5() throws IOException {
    // Test all 4 rows individually, should expect entire file
    final List<Range> ranges = List.of(new Range("r_000000", true, "r_000001", false),
        new Range("r_000001", true, "r_000002", false),
        new Range("r_000002", true, "r_000003", false),
        new Range("r_000003", true, "r_000004", false));

    assertEquals(1024, testFencing(ranges, List.of(new Range())));
  }

  @Test
  public void testFencing6() throws IOException {
    // Set range to 2.5 rows out of 4
    // Skip row 0, start row 1 and CF 2 (middle row 1), include row 3/4)
    Key start = Key.builder().row("r_000001").family("cf_000002").build();

    // Create a fence that starts at partial row 1
    final List<Range> ranges = List.of(new Range(start, true, null, true));

    // 2.5 rows equals 640 keys as each row contains 256 mutations (1024 total across all 4 rows)
    assertEquals(640, testFencing(ranges, ranges));
  }

  @Test
  public void testFencing7() throws IOException {
    // Set range to 3/4 of 1 row spanning part of row 1 and row 2
    Key start = Key.builder().row("r_000001").family("cf_000002").build();
    Key end = Key.builder().row("r_000002").family("cf_000001").build();

    // Create a fence
    final List<Range> ranges = List.of(new Range(start, true, end, true));

    // 3/4 of 1 rows equals 192 keys as each row contains 256 mutations
    assertEquals(192, testFencing(ranges, ranges));
  }

  @Test
  public void testFencing8() throws IOException {
    // Test exclusive end key

    // Create a fence for 2 rows
    final List<Range> ranges = List.of(new Range("r_000001", true, "r_000003", false));

    // None of row 2 should be included because end key is exclusive
    assertEquals(512, testFencing(ranges, ranges));
  }

  @Test
  public void testFencing9() throws IOException {
    // Test out of order ranges that should still cover whole file.
    final List<Range> ranges = List.of(new Range("r_000002", true, "r_000003", false),
        new Range("r_000003", true, "r_000004", false),
        new Range("r_000000", true, "r_000001", false),
        new Range("r_000001", true, "r_000002", false));

    assertEquals(1024, testFencing(ranges, List.of(new Range())));
  }

  @Test
  public void testFencing10() throws IOException {
    // Test overlap - 2 rows
    final List<Range> ranges = List.of(new Range("r_000002", true, "r_000003", false),
        new Range("r_000002", true, "r_000004", false));

    assertEquals(512, testFencing(ranges, ranges));
  }

  private int testFencing(List<Range> fencedRange, List<Range> expectedRange) throws IOException {
    // test an rfile with multiple rows having multiple columns

    final TestRFile trf = new RangedTestRfile(conf, fencedRange);

    trf.openWriter();

    ArrayList<Key> expectedKeys = new ArrayList<>(10000);
    ArrayList<Value> expectedValues = new ArrayList<>(10000);

    write(trf, expectedKeys, expectedValues, expectedRange);

    trf.closeWriter();

    trf.openReader();
    // seek before everything
    trf.iter.seek(new Range((Key) null, null), EMPTY_COL_FAMS, false);
    verify(trf, expectedKeys.iterator(), expectedValues.iterator());

    // seek to the middle
    int index = expectedKeys.size() / 2;
    trf.seek(expectedKeys.get(index));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // seek the first key
    index = 0;
    trf.seek(expectedKeys.get(index));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // seek to the last key
    index = expectedKeys.size() - 1;
    trf.seek(expectedKeys.get(index));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // seek after everything
    index = expectedKeys.size();
    trf.seek(new Key(new Text("z")));
    verify(trf, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // test seeking to the current location
    index = expectedKeys.size() / 2;
    trf.seek(expectedKeys.get(index));
    assertTrue(trf.iter.hasTop());
    assertEquals(expectedKeys.get(index), trf.iter.getTopKey());
    assertEquals(expectedValues.get(index), trf.iter.getTopValue());

    trf.iter.next();
    index++;
    assertTrue(trf.iter.hasTop());
    assertEquals(expectedKeys.get(index), trf.iter.getTopKey());
    assertEquals(expectedValues.get(index), trf.iter.getTopValue());

    trf.seek(expectedKeys.get(index));

    assertTrue(trf.iter.hasTop());
    assertEquals(expectedKeys.get(index), trf.iter.getTopKey());
    assertEquals(expectedValues.get(index), trf.iter.getTopValue());

    // test seeking to each location in the file
    index = 0;
    for (Key key : expectedKeys) {
      trf.seek(key);
      assertTrue(trf.iter.hasTop());
      assertEquals(key, trf.iter.getTopKey());
      assertEquals(expectedValues.get(index), trf.iter.getTopValue());

      if (index > 0) {
        // Key pkey =
        expectedKeys.get(index - 1);
        // assertEquals(pkey, trf.reader.getPrevKey());
      }

      index++;
    }

    // test seeking backwards to each key
    for (int i = expectedKeys.size() - 1; i >= 0; i--) {
      Key key = expectedKeys.get(i);

      trf.seek(key);
      assertTrue(trf.iter.hasTop());
      assertEquals(key, trf.iter.getTopKey());
      assertEquals(expectedValues.get(i), trf.iter.getTopValue());

      if (i - 1 > 0) {
        // Key pkey =
        expectedKeys.get(i - 1);
        // assertEquals(pkey, trf.reader.getPrevKey());
      }
    }

    // test seeking to random location and reading all data from that point
    // there was an off by one bug with this in the transient index
    for (int i = 0; i < 12; i++) {
      index = random.nextInt(expectedKeys.size());
      trf.seek(expectedKeys.get(index));
      for (; index < expectedKeys.size(); index++) {
        assertTrue(trf.iter.hasTop());
        assertEquals(expectedKeys.get(index), trf.iter.getTopKey());
        assertEquals(expectedValues.get(index), trf.iter.getTopValue());
        trf.iter.next();
      }
    }

    trf.closeReader();

    return expectedKeys.size();
  }

  private void write(final TestRFile trf, final List<Key> expectedKeys,
      final List<Value> expectedValues, List<Range> expected) throws IOException {
    int val = 0;

    for (int row = 0; row < 4; row++) {
      String rowS = formatString("r_", row);
      for (int cf = 0; cf < 4; cf++) {
        String cfS = formatString("cf_", cf);
        for (int cq = 0; cq < 4; cq++) {
          String cqS = formatString("cq_", cq);
          for (int cv = 'A'; cv < 'A' + 4; cv++) {
            String cvS = "" + (char) cv;
            for (int ts = 4; ts > 0; ts--) {
              Key k = newKey(rowS, cfS, cqS, cvS, ts);
              // check below ensures when all key sizes are same more than one index block is
              // created
              assertEquals(27, k.getSize());
              Value v = newValue("" + val);
              trf.writer.append(k, v);
              final Key finalK = k;
              if (expected.stream().anyMatch(range -> range.contains(finalK))) {
                expectedKeys.add(k);
                expectedValues.add(v);
              }
              val++;
            }
          }
        }
      }
    }
  }

  @Override
  protected RFile.RFileReader newReader(CachableBlockFile.CachableBuilder cb) throws IOException {
    return new RFile.RangedReader(cb);
  }
}
