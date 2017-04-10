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
package org.apache.accumulo.core.iterators.user;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.impl.BaseIteratorEnvironment;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TransformingIteratorTest {

  private static Authorizations authorizations = new Authorizations("vis0", "vis1", "vis2", "vis3", "vis4");
  private static final Map<String,String> EMPTY_OPTS = ImmutableMap.of();
  private TransformingIterator titer;

  private TreeMap<Key,Value> data = new TreeMap<>();

  @Before
  public void createData() throws Exception {
    data.clear();
    generateRow(data, "row1");
    generateRow(data, "row2");
    generateRow(data, "row3");
  }

  private void setUpTransformIterator(Class<? extends TransformingIterator> clazz) throws IOException {
    setUpTransformIterator(clazz, true);
  }

  private void setUpTransformIterator(Class<? extends TransformingIterator> clazz, boolean setupAuths) throws IOException {
    SortedMapIterator source = new SortedMapIterator(data);
    ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(source);
    SortedKeyValueIterator<Key,Value> visFilter = VisibilityFilter.wrap(cfsi, authorizations, new byte[0]);
    ReuseIterator reuserIter = new ReuseIterator();
    reuserIter.init(visFilter, EMPTY_OPTS, null);
    try {
      titer = clazz.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    IteratorEnvironment iterEnv = EasyMock.createMock(IteratorEnvironment.class);
    EasyMock.expect(iterEnv.getIteratorScope()).andReturn(IteratorScope.scan).anyTimes();
    EasyMock.replay(iterEnv);

    Map<String,String> opts;
    if (setupAuths) {
      IteratorSetting cfg = new IteratorSetting(21, clazz);
      TransformingIterator.setAuthorizations(cfg, new Authorizations("vis0", "vis1", "vis2", "vis3"));
      opts = cfg.getOptions();
    } else {
      opts = ImmutableMap.of();
    }
    titer.init(reuserIter, opts, iterEnv);
  }

  @Test
  public void testIdentityScan() throws Exception {
    setUpTransformIterator(IdentityKeyTransformingIterator.class);

    // This is just an identity scan, but with the "reuse" iterator that reuses
    // the same key/value pair for every getTopKey/getTopValue call. The code
    // will always return the final key/value if we didn't copy the original key
    // in the iterator.
    TreeMap<Key,Value> expected = new TreeMap<>();
    for (int row = 1; row <= 3; ++row) {
      for (int cf = 1; cf <= 3; ++cf) {
        for (int cq = 1; cq <= 3; ++cq) {
          for (int cv = 1; cv <= 3; ++cv) {
            putExpected(expected, row, cf, cq, cv, null);
          }
        }
      }
    }

    checkExpected(expected);
  }

  @Test
  public void testNoRangeScan() throws Exception {
    List<Class<? extends ReversingKeyTransformingIterator>> classes = new ArrayList<>();
    classes.add(ColFamReversingKeyTransformingIterator.class);
    classes.add(ColQualReversingKeyTransformingIterator.class);
    classes.add(ColVisReversingKeyTransformingIterator.class);

    // Test transforming col fam, col qual, col vis
    for (Class<? extends ReversingKeyTransformingIterator> clazz : classes) {
      setUpTransformIterator(clazz);

      // All rows with visibilities reversed
      TransformingIterator iter = clazz.newInstance();
      TreeMap<Key,Value> expected = new TreeMap<>();
      for (int row = 1; row <= 3; ++row) {
        for (int cf = 1; cf <= 3; ++cf) {
          for (int cq = 1; cq <= 3; ++cq) {
            for (int cv = 1; cv <= 3; ++cv) {
              putExpected(expected, row, cf, cq, cv, iter.getKeyPrefix());
            }
          }
        }
      }

      checkExpected(expected);
    }
  }

  @Test
  public void testVisbilityFiltering() throws Exception {
    // Should return nothing since we produced visibilities that can't be seen
    setUpTransformIterator(BadVisKeyTransformingIterator.class);
    checkExpected(new TreeMap<Key,Value>());

    // Do a "reverse" on the visibility (vis1 -> vis2, vis2 -> vis3, vis3 -> vis0)
    // Source data has vis1, vis2, vis3 so vis0 is a new one that is introduced.
    // Make sure it shows up in the output with the default test auths which include
    // vis0.
    setUpTransformIterator(ColVisReversingKeyTransformingIterator.class);
    TreeMap<Key,Value> expected = new TreeMap<>();
    for (int row = 1; row <= 3; ++row) {
      for (int cf = 1; cf <= 3; ++cf) {
        for (int cq = 1; cq <= 3; ++cq) {
          for (int cv = 1; cv <= 3; ++cv) {
            putExpected(expected, row, cf, cq, cv, PartialKey.ROW_COLFAM_COLQUAL);
          }
        }
      }
    }
    checkExpected(expected);
  }

  @Test
  public void testCreatingIllegalVisbility() throws Exception {
    // illegal visibility created by transform should be filtered on scan, even if evaluation is done
    setUpTransformIterator(IllegalVisKeyTransformingIterator.class, false);
    checkExpected(new TreeMap<Key,Value>());

    // ensure illegal vis is supressed when evaluations is done
    setUpTransformIterator(IllegalVisKeyTransformingIterator.class);
    checkExpected(new TreeMap<Key,Value>());
  }

  @Test
  public void testRangeStart() throws Exception {
    setUpTransformIterator(ColVisReversingKeyTransformingIterator.class);

    TreeMap<Key,Value> expected = new TreeMap<>();
    putExpected(expected, 1, 2, 2, 1, PartialKey.ROW_COLFAM_COLQUAL); // before the range start, but transforms in the range
    putExpected(expected, 1, 2, 2, 2, PartialKey.ROW_COLFAM_COLQUAL);

    checkExpected(new Range(new Key("row1", "cf2", "cq2", "vis1"), true, new Key("row1", "cf2", "cq3"), false), expected);
  }

  @Test
  public void testRangeEnd() throws Exception {
    setUpTransformIterator(ColVisReversingKeyTransformingIterator.class);

    TreeMap<Key,Value> expected = new TreeMap<>();
    // putExpected(expected, 1, 2, 2, 1, part); // transforms vis outside range end
    putExpected(expected, 1, 2, 2, 2, PartialKey.ROW_COLFAM_COLQUAL);
    putExpected(expected, 1, 2, 2, 3, PartialKey.ROW_COLFAM_COLQUAL);

    checkExpected(new Range(new Key("row1", "cf2", "cq2"), true, new Key("row1", "cf2", "cq2", "vis2"), false), expected);
  }

  @Test
  public void testPrefixRange() throws Exception {
    setUpTransformIterator(ColFamReversingKeyTransformingIterator.class);
    // Set a range that is before all of the untransformed data. However,
    // the data with untransformed col fam cf3 will transform to cf0 and
    // be inside the range.

    TreeMap<Key,Value> expected = new TreeMap<>();
    for (int cq = 1; cq <= 3; ++cq)
      for (int cv = 1; cv <= 3; ++cv)
        putExpected(expected, 1, 3, cq, cv, PartialKey.ROW);
    checkExpected(new Range(new Key("row1", "cf0"), true, new Key("row1", "cf1"), false), expected);
  }

  @Test
  public void testPostfixRange() throws Exception {
    // Set a range that's after all data and make sure we don't
    // somehow return something.
    setUpTransformIterator(ColFamReversingKeyTransformingIterator.class);
    checkExpected(new Range(new Key("row4"), null), new TreeMap<Key,Value>());
  }

  @Test
  public void testReplaceKeyParts() throws Exception {
    TransformingIterator it = new IdentityKeyTransformingIterator();
    Key originalKey = new Key("r", "cf", "cq", "cv", 42);
    originalKey.setDeleted(true);

    Key newKey = it.replaceColumnFamily(originalKey, new Text("test"));
    assertEquals(createDeleteKey("r", "test", "cq", "cv", 42), newKey);

    newKey = it.replaceColumnQualifier(originalKey, new Text("test"));
    assertEquals(createDeleteKey("r", "cf", "test", "cv", 42), newKey);

    newKey = it.replaceColumnVisibility(originalKey, new Text("test"));
    assertEquals(createDeleteKey("r", "cf", "cq", "test", 42), newKey);

    newKey = it.replaceKeyParts(originalKey, new Text("testCQ"), new Text("testCV"));
    assertEquals(createDeleteKey("r", "cf", "testCQ", "testCV", 42), newKey);

    newKey = it.replaceKeyParts(originalKey, new Text("testCF"), new Text("testCQ"), new Text("testCV"));
    assertEquals(createDeleteKey("r", "testCF", "testCQ", "testCV", 42), newKey);
  }

  @Test
  public void testFetchColumnFamilites() throws Exception {
    // In this test, we are fetching column family cf2, which is in
    // the transformed space. The source column family that will
    // transform into cf2 is cf1, so that is the column family we
    // put in the expectations.
    int expectedCF = 1;
    setUpTransformIterator(ColFamReversingKeyTransformingIterator.class);

    TreeMap<Key,Value> expected = new TreeMap<>();
    for (int row = 1; row <= 3; ++row)
      for (int cq = 1; cq <= 3; ++cq)
        for (int cv = 1; cv <= 3; ++cv)
          putExpected(expected, row, expectedCF, cq, cv, PartialKey.ROW);
    checkExpected(expected, "cf2");
  }

  @Test
  public void testDeepCopy() throws Exception {
    ColumnVisibility vis1 = new ColumnVisibility("vis1");
    ColumnVisibility vis3 = new ColumnVisibility("vis3");

    data.clear();

    Value ev = new Value("".getBytes());

    data.put(new Key("shard001", "foo", "doc02", vis1, 78), ev);
    data.put(new Key("shard001", "dog", "doc02", vis3, 78), ev);
    data.put(new Key("shard001", "cat", "doc02", vis3, 78), ev);

    data.put(new Key("shard001", "bar", "doc03", vis1, 78), ev);
    data.put(new Key("shard001", "dog", "doc03", vis3, 78), ev);
    data.put(new Key("shard001", "cat", "doc03", vis3, 78), ev);

    setUpTransformIterator(ColVisReversingKeyTransformingIterator.class);

    IntersectingIterator iiIter = new IntersectingIterator();
    IteratorSetting iicfg = new IteratorSetting(22, IntersectingIterator.class);
    IntersectingIterator.setColumnFamilies(iicfg, new Text[] {new Text("foo"), new Text("dog"), new Text("cat")});
    iiIter.init(titer, iicfg.getOptions(), null);

    iiIter.seek(new Range(), new HashSet<ByteSequence>(), false);

    assertTrue(iiIter.hasTop());
    Key docKey = iiIter.getTopKey();
    assertEquals("shard001", docKey.getRowData().toString());
    assertEquals("doc02", docKey.getColumnQualifierData().toString());
    iiIter.next();
    assertFalse(iiIter.hasTop());
  }

  @Test
  public void testCompactionScanFetchingColumnFamilies() throws Exception {
    // In this test, we are fetching column family cf2, which is in
    // the transformed space. The source column family that will
    // transform into cf2 is cf1, so that is the column family we
    // put in the expectations.
    int expectedCF = 1;
    setUpTransformIterator(ColFamReversingCompactionKeyTransformingIterator.class);

    TreeMap<Key,Value> expected = new TreeMap<>();
    for (int row = 1; row <= 3; ++row)
      for (int cq = 1; cq <= 3; ++cq)
        for (int cv = 1; cv <= 3; ++cv)
          putExpected(expected, row, expectedCF, cq, cv, PartialKey.ROW);
    checkExpected(expected, "cf2");
  }

  @Test
  public void testCompactionDoesntFilterVisibilities() throws Exception {
    // In scan mode, this should return nothing since it produces visibilites
    // the user can't see. In compaction mode, however, the visibilites
    // should still show up.
    setUpTransformIterator(BadVisCompactionKeyTransformingIterator.class);

    TreeMap<Key,Value> expected = new TreeMap<>();
    for (int rowID = 1; rowID <= 3; ++rowID) {
      for (int cfID = 1; cfID <= 3; ++cfID) {
        for (int cqID = 1; cqID <= 3; ++cqID) {
          for (int cvID = 1; cvID <= 3; ++cvID) {
            String row = "row" + rowID;
            String cf = "cf" + cfID;
            String cq = "cq" + cqID;
            String cv = "badvis";
            long ts = 100 * cfID + 10 * cqID + cvID;
            String val = "val" + ts;
            expected.put(new Key(row, cf, cq, cv, ts), new Value(val.getBytes()));
          }
        }
      }
    }

    checkExpected(expected);
  }

  @Test
  public void testCompactionAndIllegalVisibility() throws Exception {
    setUpTransformIterator(IllegalVisCompactionKeyTransformingIterator.class);
    try {
      checkExpected(new TreeMap<Key,Value>());
      assertTrue(false);
    } catch (Exception e) {

    }
  }

  @Test
  public void testDupes() throws Exception {
    setUpTransformIterator(DupeTransformingIterator.class);

    titer.seek(new Range(), new HashSet<ByteSequence>(), false);

    int count = 0;
    while (titer.hasTop()) {
      Key key = titer.getTopKey();
      titer.next();
      assertEquals("cf1", key.getColumnFamily().toString());
      assertEquals("cq1", key.getColumnQualifier().toString());
      assertEquals("", key.getColumnVisibility().toString());
      assertEquals(5l, key.getTimestamp());
      count++;
    }

    assertEquals(81, count);
  }

  @Test
  public void testValidateOptions() {
    TransformingIterator ti = new ColFamReversingKeyTransformingIterator();
    IteratorSetting is = new IteratorSetting(100, "cfrkt", ColFamReversingKeyTransformingIterator.class);
    TransformingIterator.setAuthorizations(is, new Authorizations("A", "B"));
    TransformingIterator.setMaxBufferSize(is, 10000000);
    Assert.assertTrue(ti.validateOptions(is.getOptions()));

    Map<String,String> opts = new HashMap<>();

    opts.put(TransformingIterator.MAX_BUFFER_SIZE_OPT, "10M");
    Assert.assertTrue(ti.validateOptions(is.getOptions()));

    opts.clear();
    opts.put(TransformingIterator.MAX_BUFFER_SIZE_OPT, "A,B");
    try {
      ti.validateOptions(opts);
      Assert.assertFalse(true);
    } catch (IllegalArgumentException e) {}

    opts.clear();
    opts.put(TransformingIterator.AUTH_OPT, Authorizations.HEADER + "~~~~");
    try {
      ti.validateOptions(opts);
      Assert.assertFalse(true);
    } catch (IllegalArgumentException e) {}

  }

  private Key createDeleteKey(String row, String colFam, String colQual, String colVis, long timestamp) {
    Key key = new Key(row, colFam, colQual, colVis, timestamp);
    key.setDeleted(true);
    return key;
  }

  private void checkExpected(Range range, TreeMap<Key,Value> expectedEntries) throws IOException {
    checkExpected(range, new HashSet<ByteSequence>(), expectedEntries);
  }

  private void checkExpected(TreeMap<Key,Value> expectedEntries, String... fa) throws IOException {

    HashSet<ByteSequence> families = new HashSet<>();
    for (String family : fa) {
      families.add(new ArrayByteSequence(family));
    }

    checkExpected(new Range(), families, expectedEntries);
  }

  private void checkExpected(Range range, Set<ByteSequence> families, TreeMap<Key,Value> expectedEntries) throws IOException {

    titer.seek(range, families, families.size() != 0);

    while (titer.hasTop()) {
      Entry<Key,Value> expected = expectedEntries.pollFirstEntry();
      Key actualKey = titer.getTopKey();
      Value actualValue = titer.getTopValue();
      titer.next();

      assertNotNull("Ran out of expected entries on: " + actualKey, expected);
      assertEquals("Key mismatch", expected.getKey(), actualKey);
      assertEquals("Value mismatch", expected.getValue(), actualValue);
    }

    assertTrue("Scanner did not return all expected entries: " + expectedEntries, expectedEntries.isEmpty());
  }

  private static void putExpected(SortedMap<Key,Value> expected, int rowID, int cfID, int cqID, int cvID, PartialKey part) {
    String row = "row" + rowID;
    String cf = "cf" + cfID;
    String cq = "cq" + cqID;
    String cv = "vis" + cvID;
    long ts = 100 * cfID + 10 * cqID + cvID;
    String val = "val" + ts;

    if (part != null) {
      switch (part) {
        case ROW:
          cf = transform(new Text(cf)).toString();
          break;
        case ROW_COLFAM:
          cq = transform(new Text(cq)).toString();
          break;
        case ROW_COLFAM_COLQUAL:
          cv = transform(new Text(cv)).toString();
          break;
        default:
          break;
      }
    }

    expected.put(new Key(row, cf, cq, cv, ts), new Value(val.getBytes()));
  }

  private static Text transform(Text val) {
    String s = val.toString();
    // Reverse the order of the number at the end, and subtract one
    int i = 3 - Integer.parseInt(s.substring(s.length() - 1));
    StringBuilder sb = new StringBuilder();
    sb.append(s.substring(0, s.length() - 1));
    sb.append(i);
    return new Text(sb.toString());
  }

  private static void generateRow(TreeMap<Key,Value> data, String row) {

    for (int cfID = 1; cfID <= 3; ++cfID) {
      for (int cqID = 1; cqID <= 3; ++cqID) {
        for (int cvID = 1; cvID <= 3; ++cvID) {
          String cf = "cf" + cfID;
          String cq = "cq" + cqID;
          String cv = "vis" + cvID;
          long ts = 100 * cfID + 10 * cqID + cvID;
          String val = "val" + ts;

          Key k = new Key(row, cf, cq, cv, ts);
          Value v = new Value(val.getBytes());
          data.put(k, v);
        }
      }
    }

  }

  private static Key reverseKeyPart(Key originalKey, PartialKey part) {
    Text row = originalKey.getRow();
    Text cf = originalKey.getColumnFamily();
    Text cq = originalKey.getColumnQualifier();
    Text cv = originalKey.getColumnVisibility();
    long ts = originalKey.getTimestamp();
    switch (part) {
      case ROW:
        cf = transform(cf);
        break;
      case ROW_COLFAM:
        cq = transform(cq);
        break;
      case ROW_COLFAM_COLQUAL:
        cv = transform(cv);
        break;
      default:
        break;
    }
    return new Key(row, cf, cq, cv, ts);
  }

  public static class IdentityKeyTransformingIterator extends TransformingIterator {
    @Override
    protected PartialKey getKeyPrefix() {
      return PartialKey.ROW;
    }

    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> input, KVBuffer output) throws IOException {
      while (input.hasTop()) {
        output.append(input.getTopKey(), input.getTopValue());
        input.next();
      }
    }
  }

  public static class DupeTransformingIterator extends TransformingIterator {
    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> input, KVBuffer output) throws IOException {
      while (input.hasTop()) {
        Key originalKey = input.getTopKey();
        Key ret = replaceKeyParts(originalKey, new Text("cf1"), new Text("cq1"), new Text(""));
        ret.setTimestamp(5);
        output.append(ret, input.getTopValue());
        input.next();
      }
    }

    @Override
    protected PartialKey getKeyPrefix() {
      return PartialKey.ROW;
    }

  }

  public static abstract class ReversingKeyTransformingIterator extends TransformingIterator {

    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> input, KVBuffer output) throws IOException {
      while (input.hasTop()) {
        Key originalKey = input.getTopKey();
        output.append(reverseKeyPart(originalKey, getKeyPrefix()), input.getTopValue());
        input.next();
      }
    }
  }

  public static class ColFamReversingKeyTransformingIterator extends ReversingKeyTransformingIterator {
    @Override
    protected PartialKey getKeyPrefix() {
      return PartialKey.ROW;
    }

    @Override
    protected Collection<ByteSequence> untransformColumnFamilies(Collection<ByteSequence> columnFamilies) {
      HashSet<ByteSequence> untransformed = new HashSet<>();
      for (ByteSequence cf : columnFamilies)
        untransformed.add(untransformColumnFamily(cf));
      return untransformed;
    }

    protected ByteSequence untransformColumnFamily(ByteSequence colFam) {
      Text transformed = transform(new Text(colFam.toArray()));
      byte[] bytes = transformed.getBytes();
      return new ArrayByteSequence(bytes, 0, transformed.getLength());
    }
  }

  public static class ColFamReversingCompactionKeyTransformingIterator extends ColFamReversingKeyTransformingIterator {
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      env = new MajCIteratorEnvironmentAdapter();
      super.init(source, options, env);
    }
  }

  public static class ColQualReversingKeyTransformingIterator extends ReversingKeyTransformingIterator {
    @Override
    protected PartialKey getKeyPrefix() {
      return PartialKey.ROW_COLFAM;
    }
  }

  public static class ColVisReversingKeyTransformingIterator extends ReversingKeyTransformingIterator {
    @Override
    protected PartialKey getKeyPrefix() {
      return PartialKey.ROW_COLFAM_COLQUAL;
    }
  }

  public static class IllegalVisKeyTransformingIterator extends TransformingIterator {
    @Override
    protected PartialKey getKeyPrefix() {
      return PartialKey.ROW_COLFAM_COLQUAL;
    }

    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> input, KVBuffer output) throws IOException {
      while (input.hasTop()) {
        Key originalKey = input.getTopKey();
        output.append(
            new Key(originalKey.getRow(), originalKey.getColumnFamily(), originalKey.getColumnQualifier(), new Text("A&|||"), originalKey.getTimestamp()),
            input.getTopValue());
        input.next();
      }
    }
  }

  public static class IllegalVisCompactionKeyTransformingIterator extends IllegalVisKeyTransformingIterator {
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      env = new MajCIteratorEnvironmentAdapter();
      super.init(source, options, env);
    }
  }

  public static class BadVisKeyTransformingIterator extends TransformingIterator {
    @Override
    protected PartialKey getKeyPrefix() {
      return PartialKey.ROW_COLFAM_COLQUAL;
    }

    @Override
    protected void transformRange(SortedKeyValueIterator<Key,Value> input, KVBuffer output) throws IOException {
      while (input.hasTop()) {
        Key originalKey = input.getTopKey();
        output.append(
            new Key(originalKey.getRow(), originalKey.getColumnFamily(), originalKey.getColumnQualifier(), new Text("badvis"), originalKey.getTimestamp()),
            input.getTopValue());
        input.next();
      }
    }
  }

  public static class BadVisCompactionKeyTransformingIterator extends BadVisKeyTransformingIterator {
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      env = new MajCIteratorEnvironmentAdapter();
      super.init(source, options, env);
    }
  }

  public static class ReuseIterator extends WrappingIterator {
    private Key topKey = new Key();
    private Value topValue = new Value();

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      ReuseIterator rei = new ReuseIterator();
      rei.setSource(getSource().deepCopy(env));
      return rei;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      super.seek(range, columnFamilies, inclusive);
      loadTop();
    }

    @Override
    public void next() throws IOException {
      super.next();
      loadTop();
    }

    @Override
    public Key getTopKey() {
      return topKey;
    }

    @Override
    public Value getTopValue() {
      return topValue;
    }

    private void loadTop() {
      if (hasTop()) {
        topKey.set(super.getTopKey());
        topValue.set(super.getTopValue().get());
      }
    }
  }

  private static class MajCIteratorEnvironmentAdapter extends BaseIteratorEnvironment {
    @Override
    public IteratorScope getIteratorScope() {
      return IteratorScope.majc;
    }
  }
}
