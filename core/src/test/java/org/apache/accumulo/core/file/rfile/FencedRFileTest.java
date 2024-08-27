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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.crypto.CryptoTest;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile.FencedIndex;
import org.apache.accumulo.core.file.rfile.RFile.FencedReader;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FencedRFileTest extends AbstractRFileTest {

  @BeforeAll
  public static void setupCryptoKeyFile() throws Exception {
    CryptoTest.setupKeyFiles(FencedRFileTest.class);
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
    // Expect entire all rows to be seen as first range end key is inclusive
    assertEquals(1024,
        testFencing(List.of(new Range(null, new Key("r_000002")), new Range("r_000002", null)),
            List.of(new Range())));
  }

  @Test
  public void testFencing3() throws IOException {
    // Create a fence that contains only row 0 row 2
    // Expect only to see values from those two rows and not row 1 or row 3
    final List<Range> ranges = List.of(new Range("r_000000"), new Range("r_000002"));

    // Use the same range for the fence and testing to make sure only the expected keys were seen
    // Should only be 512 keys as 2 rows * 256
    assertEquals(512, testFencing(ranges, ranges));
  }

  // Should fail
  @Test
  public void testFencing4() throws IOException {
    // Create a fence that contains row 0 and row 2 only
    final List<Range> ranges = List.of(new Range("r_000000"), new Range("r_000002"));

    // Expected range contains only row 2 so should fail as row 1 should also be seen
    final List<Range> ranges2 = List.of(new Range("r_000002"));

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
    final List<Range> ranges = List.of(new Range("r_000000"), new Range("r_000001"),
        new Range("r_000002"), new Range("r_000003"));

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
    // Create a fence for 2 rows
    final List<Range> ranges = List.of(new Range("r_000001", true, "r_000002", true));

    // Should only be rows 1 and 2
    assertEquals(512, testFencing(ranges, ranges));
  }

  @Test
  public void testFencing9() throws IOException {
    // Test out of order ranges that should still cover whole file.
    final List<Range> ranges = List.of(new Range("r_000002"), new Range("r_000003"),
        new Range("r_000000"), new Range("r_000001"));

    assertEquals(1024, testFencing(ranges, List.of(new Range())));
  }

  @Test
  public void testFencing10() throws IOException {
    // Test overlap 2 rows that are merged
    final List<Range> ranges = Range.mergeOverlapping(
        List.of(new Range("r_000002"), new Range("r_000002", true, "r_000003", true)));

    assertEquals(512, testFencing(ranges, ranges));
  }

  @Test
  public void testFencing11() throws IOException {
    // Test fence covering just a single row
    final List<Range> ranges = List.of(new Range("r_000001"));

    // should be 256 keys in row r_000001
    assertEquals(256, testFencing(ranges, ranges));
  }

  @Test
  public void testFencing12() throws IOException {
    final TestRFile trf = initTestFile();

    // Fence off the file to contain only 1 row (r_00001)
    Range range = new Range("r_000001");
    trf.openReader(range);

    // Open a fenced reader
    final SortedKeyValueIterator<Key,Value> iter = trf.iter;
    assertTrue(iter instanceof FencedReader);

    // Seek to the row that is part of the fence
    seek(iter, new Key(new Text("r_000001")));
    assertTrue(iter.hasTop());

    // each row has 256 keys, read 1/4 of the keys
    // and verify hasTop() is true
    for (int i = 0; i < 64; i++) {
      iter.next();
      assertTrue(iter.hasTop());
    }

    // Seek to a range that is disjoint. The fence only covers
    // row r_000001 as end row is exclusive so seeking to row r_000002
    // should result in hasTop() returning false
    seek(iter, new Key(new Text("r_000002")));
    // Verify hasTop() is now false
    assertFalse(iter.hasTop());
  }

  @Test
  public void testFirstAndLastRow() throws IOException {
    final TestRFile trf = initTestFile();

    Text firstRowInFile = new Text(formatString("r_", 0));
    Text lastRowInFile = new Text(formatString("r_", 3));

    // Infinite range fence
    // Should just be first/last rows of file
    assertReader(trf, new Range(), (reader) -> {
      assertEquals(firstRowInFile, reader.getFirstRow());
      assertEquals(lastRowInFile, reader.getLastRow());
    });

    // Range inside of file so should return the rows of the fence
    assertReader(trf, new Range("r_000001", "r_000002"), (reader) -> {
      assertEquals(new Text("r_000001"), reader.getFirstRow());
      assertEquals(new Text("r_000002"), reader.getLastRow());
    });

    // Test infinite start row
    assertReader(trf, new Range(null, "r_000001"), (reader) -> {
      assertEquals(firstRowInFile, reader.getFirstRow());
      assertEquals(new Text("r_000001"), reader.getLastRow());
    });

    // Test infinite end row
    assertReader(trf, new Range("r_000002", null), (reader) -> {
      assertEquals(new Text("r_000002"), reader.getFirstRow());
      assertEquals(lastRowInFile, reader.getLastRow());
    });

    // Test start row matches start of file
    assertReader(trf, new Range("r_000000", "r_000002"), (reader) -> {
      // start row of range matches first row in file so that should be returned instead
      assertEquals(firstRowInFile, reader.getFirstRow());
      assertEquals(new Text("r_000002"), reader.getLastRow());
    });

    // Test end row matches end of file
    assertReader(trf, new Range("r_000001", "r_000003"), (reader) -> {
      assertEquals(new Text("r_000001"), reader.getFirstRow());
      // end row of range matches last row in file so that should be returned instead
      assertEquals(lastRowInFile, reader.getLastRow());
    });

    // Test case where rows in range are less than and greater than rows in file
    assertReader(trf, new Range("a", "z"), (reader) -> {
      assertEquals(firstRowInFile, reader.getFirstRow());
      assertEquals(lastRowInFile, reader.getLastRow());
    });

    // Test inclusive end key, usually a row range is required to be an exclusive key
    // for a tablet file but the fenced reader still supports any range type
    assertReader(trf, new Range(new Key("r_000002"), true, new Key("r_000002"), true), (reader) -> {
      assertEquals(new Text("r_000002"), reader.getFirstRow());
      assertEquals(new Text("r_000002"), reader.getLastRow());
    });

  }

  @Test
  public void testUnsupportedMethods() throws IOException {
    final TestRFile trf = initTestFile();
    trf.openReader(new Range());
    FencedReader reader = (FencedReader) trf.iter;
    FencedIndex index = (FencedIndex) reader.getIndex();

    assertThrows(UnsupportedOperationException.class, () -> reader.init(null, null, null));
    assertThrows(UnsupportedOperationException.class,
        () -> index.getSample(new SamplerConfigurationImpl()));
    assertThrows(UnsupportedOperationException.class,
        () -> index.seek(new Range(), List.of(), false));
    assertThrows(UnsupportedOperationException.class, () -> index.deepCopy(null));
  }

  @Test
  public void testSetInterrupted() throws IOException {
    final TestRFile trf = initTestFile();
    trf.openReader(new Range());
    FencedReader reader = (FencedReader) trf.iter;

    reader.setInterruptFlag(new AtomicBoolean(true));
    assertThrows(IterationInterruptedException.class,
        () -> reader.seek(new Range("r_000001"), List.of(), false));

  }

  @Test
  public void testReset() throws IOException {
    final TestRFile trf = initTestFile();
    trf.openReader(new Range());
    FencedReader reader = (FencedReader) trf.iter;

    assertFalse(reader.hasTop());
    reader.seek(new Range("r_000001"), List.of(), false);
    assertTrue(reader.hasTop());
    assertEquals(
        newKey(formatString("r_", 1), formatString("cf_", 0), formatString("cq_", 0), "A", 4),
        reader.getTopKey());

    reader.reset();
    assertFalse(reader.hasTop());
  }

  @Test
  public void testEstimateOverlappingEntries() throws IOException {
    final TestRFile trf = new TestRFile(conf);
    // set block sizes lower to get a better estimate
    trf.openWriter(true, 100, 100);
    // write a test file with 1024 entries
    writeTestFile(trf);
    trf.closeWriter();

    // Test with infinite start/end range for fenced file
    Range range = new Range();
    trf.openReader(range);
    FencedReader reader = (FencedReader) trf.iter;
    // Expect entire range to be seen, so we can re-use the same verifyEstimate() tests
    // used for non-fenced files
    verifyEstimated(reader);
    trf.closeReader();

    // Test with start/end range for fenced file that covers full file
    range = new Range(null, false, "r_000004", true);
    trf.openReader(range);
    reader = (FencedReader) trf.iter;
    verifyEstimated(reader);
    trf.closeReader();

    // Fence off the file to contain only 1 row (r_00001)
    range = new Range("r_000001", false, "r_000002", true);
    trf.openReader(range);
    reader = (FencedReader) trf.iter;

    // Key extent is null end/prev end row but file is fenced to 1 row
    var extent = new KeyExtent(TableId.of("1"), null, null);
    long estimated = reader.estimateOverlappingEntries(extent);
    // One row contains 256 but will overestimate slightly
    assertEquals(258, estimated);

    // Disjoint, fenced file is set to row 1 and KeyExtent is row 0
    estimated = reader
        .estimateOverlappingEntries(new KeyExtent(TableId.of("1"), new Text("r_000001"), null));
    assertEquals(0, estimated);
    trf.closeReader();

    // Fence off the file to contain only 2 rows (r_00001 and r_000002)
    range = new Range("r_000001", false, "r_000003", true);
    trf.openReader(range);
    reader = (FencedReader) trf.iter;

    // Key extent is null end/prev end row but file is fenced to 2 rows
    extent = new KeyExtent(TableId.of("1"), null, null);
    estimated = reader.estimateOverlappingEntries(extent);
    // two rows contain 512 but will overestimate slightly
    assertEquals(514, estimated);

    // 1 overlapping row
    extent = new KeyExtent(TableId.of("1"), new Text("r_000002"), null);
    estimated = reader.estimateOverlappingEntries(extent);
    assertEquals(258, estimated);
    trf.closeReader();

    // Invalid row range
    range = new Range("r_000001", true, "r_000003", true);
    trf.openReader(range);
    final var r = (FencedReader) trf.iter;
    assertThrows(IllegalArgumentException.class,
        () -> r.estimateOverlappingEntries(new KeyExtent(TableId.of("1"), null, null)));
    trf.closeReader();
  }

  private int testFencing(List<Range> fencedRange, List<Range> expectedRange) throws IOException {
    // test an rfile with multiple rows having multiple columns

    final ArrayList<Key> expectedKeys = new ArrayList<>(10000);
    final ArrayList<Value> expectedValues = new ArrayList<>(10000);

    final List<TestRFile> rangedTrfs = new ArrayList<>();
    final List<SortedKeyValueIterator<Key,Value>> rangedIters = new ArrayList<>();

    // For each range build a new test rfile and ranged reader for it
    // We have to copy the data for each range for the test
    for (Range range : fencedRange) {
      expectedKeys.clear();
      expectedValues.clear();
      final TestRFile trf = new TestRFile(conf);
      trf.openWriter();
      writeTestFile(trf, expectedKeys, expectedValues, expectedRange);
      trf.closeWriter();
      rangedTrfs.add(trf);
      trf.openReader(range);
      rangedIters.add(trf.iter);
    }

    final MultiIterator trfIter = new MultiIterator(rangedIters, false);

    // seek before everything
    trfIter.seek(new Range((Key) null, null), EMPTY_COL_FAMS, false);
    verify(trfIter, expectedKeys.iterator(), expectedValues.iterator());

    // seek to the middle
    int index = expectedKeys.size() / 2;
    seek(trfIter, expectedKeys.get(index));
    verify(trfIter, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // seek the first key
    index = 0;
    seek(trfIter, expectedKeys.get(index));
    verify(trfIter, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // seek to the last key
    index = expectedKeys.size() - 1;
    seek(trfIter, expectedKeys.get(index));
    verify(trfIter, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // seek after everything
    index = expectedKeys.size();
    seek(trfIter, new Key(new Text("z")));
    verify(trfIter, expectedKeys.subList(index, expectedKeys.size()).iterator(),
        expectedValues.subList(index, expectedKeys.size()).iterator());

    // test seeking to the current location
    index = expectedKeys.size() / 2;
    seek(trfIter, expectedKeys.get(index));
    assertTrue(trfIter.hasTop());
    assertEquals(expectedKeys.get(index), trfIter.getTopKey());
    assertEquals(expectedValues.get(index), trfIter.getTopValue());

    trfIter.next();
    index++;
    assertTrue(trfIter.hasTop());
    assertEquals(expectedKeys.get(index), trfIter.getTopKey());
    assertEquals(expectedValues.get(index), trfIter.getTopValue());

    seek(trfIter, expectedKeys.get(index));

    assertTrue(trfIter.hasTop());
    assertEquals(expectedKeys.get(index), trfIter.getTopKey());
    assertEquals(expectedValues.get(index), trfIter.getTopValue());

    // test seeking to each location in the file
    index = 0;
    for (Key key : expectedKeys) {
      seek(trfIter, key);
      assertTrue(trfIter.hasTop());
      assertEquals(key, trfIter.getTopKey());
      assertEquals(expectedValues.get(index), trfIter.getTopValue());
      index++;
    }

    // test seeking backwards to each key
    for (int i = expectedKeys.size() - 1; i >= 0; i--) {
      Key key = expectedKeys.get(i);

      seek(trfIter, key);
      assertTrue(trfIter.hasTop());
      assertEquals(key, trfIter.getTopKey());
      assertEquals(expectedValues.get(i), trfIter.getTopValue());

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
      seek(trfIter, expectedKeys.get(index));
      for (; index < expectedKeys.size(); index++) {
        assertTrue(trfIter.hasTop());
        assertEquals(expectedKeys.get(index), trfIter.getTopKey());
        assertEquals(expectedValues.get(index), trfIter.getTopValue());
        trfIter.next();
      }
    }

    for (TestRFile rangedTrf : rangedTrfs) {
      // check index entries to verify within range
      FileSKVIterator iiter = ((FencedReader) rangedTrf.iter).getIndex();
      while (iiter.hasTop()) {
        assertTrue(expectedRange.stream().anyMatch(range -> range.contains(iiter.getTopKey())));
        iiter.next();
      }
      rangedTrf.closeReader();
    }

    return expectedKeys.size();
  }

  private static void seek(SortedKeyValueIterator<Key,Value> iter, Key nk) throws IOException {
    iter.seek(new Range(nk, null), EMPTY_COL_FAMS, false);
  }

  private void writeTestFile(final TestRFile trf) throws IOException {
    writeTestFile(trf, null, null, null);
  }

  private void writeTestFile(final TestRFile trf, final List<Key> expectedKeys,
      final List<Value> expectedValues, List<Range> expectedRange) throws IOException {
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
              Optional.ofNullable(expectedRange).ifPresent(expected -> {
                if (expected.stream().anyMatch(range -> range.contains(finalK))) {
                  expectedKeys.add(k);
                  expectedValues.add(v);
                }
              });
              val++;
            }
          }
        }
      }
    }
  }

  private TestRFile initTestFile() throws IOException {
    final TestRFile trf = new TestRFile(conf);
    trf.openWriter();
    writeTestFile(trf);
    trf.closeWriter();
    return trf;
  }

  private static void assertReader(final TestRFile trf, Range range,
      ThrowableConsumer<FencedReader,IOException> run) throws IOException {
    FencedReader reader = null;
    try {
      trf.openReader(range);
      reader = (FencedReader) trf.iter;
      run.accept(reader);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

  }

  // Similar to the java.util.function.Consumer interface but throws an exception
  interface ThrowableConsumer<T,U extends Throwable> {
    void accept(T t) throws U;
  }

}
