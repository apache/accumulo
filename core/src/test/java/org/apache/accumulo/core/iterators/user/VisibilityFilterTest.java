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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class VisibilityFilterTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

  private static final Text BAD = new Text("bad");
  private static final Text GOOD = new Text("good");
  private static final Text EMPTY_VIS = new Text("");
  private static final Text GOOD_VIS = new Text("abc|def");
  private static final Text HIDDEN_VIS = new Text("abc&def&ghi");
  private static final Text BAD_VIS = new Text("&");
  private static final Value EMPTY_VALUE = new Value(new byte[0]);

  private TreeMap<Key,Value> createUnprotectedSource(int numPublic, int numHidden) {
    TreeMap<Key,Value> source = new TreeMap<Key,Value>();
    for (int i = 0; i < numPublic; i++)
      source.put(new Key(new Text(String.format("%03d", i)), GOOD, GOOD, EMPTY_VIS), EMPTY_VALUE);
    for (int i = 0; i < numHidden; i++)
      source.put(new Key(new Text(String.format("%03d", i)), BAD, BAD, GOOD_VIS), EMPTY_VALUE);
    return source;
  }

  private TreeMap<Key,Value> createPollutedSource(int numGood, int numBad) {
    TreeMap<Key,Value> source = new TreeMap<Key,Value>();
    for (int i = 0; i < numGood; i++)
      source.put(new Key(new Text(String.format("%03d", i)), GOOD, GOOD, GOOD_VIS), EMPTY_VALUE);
    for (int i = 0; i < numBad; i++)
      source.put(new Key(new Text(String.format("%03d", i)), BAD, BAD, BAD_VIS), EMPTY_VALUE);
    return source;
  }

  private TreeMap<Key,Value> createSourceWithHiddenData(int numViewable, int numHidden) {
    TreeMap<Key,Value> source = new TreeMap<Key,Value>();
    for (int i = 0; i < numViewable; i++)
      source.put(new Key(new Text(String.format("%03d", i)), GOOD, GOOD, GOOD_VIS), EMPTY_VALUE);
    for (int i = 0; i < numHidden; i++)
      source.put(new Key(new Text(String.format("%03d", i)), BAD, BAD, HIDDEN_VIS), EMPTY_VALUE);
    return source;
  }

  private void verify(TreeMap<Key,Value> source, int expectedSourceSize, Map<String,String> options, Text expectedCF, Text expectedCQ, Text expectedCV,
      int expectedFinalCount) throws IOException {
    assertEquals(expectedSourceSize, source.size());

    Filter filter = new VisibilityFilter();
    filter.init(new SortedMapIterator(source), options, null);
    filter.seek(new Range(), EMPTY_COL_FAMS, false);

    int count = 0;
    while (filter.hasTop()) {
      count++;
      // System.out.println(DefaultFormatter.formatEntry(Collections.singletonMap(filter.getTopKey(), filter.getTopValue()).entrySet().iterator().next(),
      // false));
      assertEquals(expectedCF, filter.getTopKey().getColumnFamily());
      assertEquals(expectedCQ, filter.getTopKey().getColumnQualifier());
      assertEquals(expectedCV, filter.getTopKey().getColumnVisibility());
      filter.next();
    }
    assertEquals(expectedFinalCount, count);
  }

  @Test
  public void testAllowValidLabelsOnly() throws IOException {
    IteratorSetting is = new IteratorSetting(1, VisibilityFilter.class);
    VisibilityFilter.filterInvalidLabelsOnly(is, true);

    TreeMap<Key,Value> source = createPollutedSource(1, 2);
    verify(source, 3, is.getOptions(), GOOD, GOOD, GOOD_VIS, 1);

    source = createPollutedSource(30, 500);
    verify(source, 530, is.getOptions(), GOOD, GOOD, GOOD_VIS, 30);

    source = createPollutedSource(1000, 500);
    verify(source, 1500, is.getOptions(), GOOD, GOOD, GOOD_VIS, 1000);
  }

  @Test
  public void testAllowBadLabelsOnly() throws IOException {
    IteratorSetting is = new IteratorSetting(1, VisibilityFilter.class);
    VisibilityFilter.setNegate(is, true);
    VisibilityFilter.filterInvalidLabelsOnly(is, true);

    TreeMap<Key,Value> source = createPollutedSource(1, 2);
    verify(source, 3, is.getOptions(), BAD, BAD, BAD_VIS, 2);

    source = createPollutedSource(30, 500);
    verify(source, 530, is.getOptions(), BAD, BAD, BAD_VIS, 500);

    source = createPollutedSource(1000, 500);
    verify(source, 1500, is.getOptions(), BAD, BAD, BAD_VIS, 500);
  }

  @Test
  public void testAllowAuthorizedLabelsOnly() throws IOException {
    IteratorSetting is = new IteratorSetting(1, VisibilityFilter.class);
    VisibilityFilter.setAuthorizations(is, new Authorizations("def"));

    TreeMap<Key,Value> source = createSourceWithHiddenData(1, 2);
    verify(source, 3, is.getOptions(), GOOD, GOOD, GOOD_VIS, 1);

    source = createSourceWithHiddenData(30, 500);
    verify(source, 530, is.getOptions(), GOOD, GOOD, GOOD_VIS, 30);

    source = createSourceWithHiddenData(1000, 500);
    verify(source, 1500, is.getOptions(), GOOD, GOOD, GOOD_VIS, 1000);
  }

  @Test
  public void testAllowUnauthorizedLabelsOnly() throws IOException {
    IteratorSetting is = new IteratorSetting(1, VisibilityFilter.class);
    VisibilityFilter.setNegate(is, true);
    VisibilityFilter.setAuthorizations(is, new Authorizations("def"));

    TreeMap<Key,Value> source = createSourceWithHiddenData(1, 2);
    verify(source, 3, is.getOptions(), BAD, BAD, HIDDEN_VIS, 2);

    source = createSourceWithHiddenData(30, 500);
    verify(source, 530, is.getOptions(), BAD, BAD, HIDDEN_VIS, 500);

    source = createSourceWithHiddenData(1000, 500);
    verify(source, 1500, is.getOptions(), BAD, BAD, HIDDEN_VIS, 500);
  }

  @Test
  public void testNoLabels() throws IOException {
    IteratorSetting is = new IteratorSetting(1, VisibilityFilter.class);
    VisibilityFilter.setNegate(is, false);
    VisibilityFilter.setAuthorizations(is, new Authorizations());

    TreeMap<Key,Value> source = createUnprotectedSource(5, 2);
    verify(source, 7, is.getOptions(), GOOD, GOOD, EMPTY_VIS, 5);

    VisibilityFilter.setNegate(is, true);
    verify(source, 7, is.getOptions(), BAD, BAD, GOOD_VIS, 2);
  }

  @Test
  public void testFilterUnauthorizedAndBad() throws IOException {
    /*
     * if not explicitly filtering bad labels, they will still be filtered while validating against authorizations, but it will be very verbose in the logs
     */
    IteratorSetting is = new IteratorSetting(1, VisibilityFilter.class);
    VisibilityFilter.setAuthorizations(is, new Authorizations("def"));

    TreeMap<Key,Value> source = createSourceWithHiddenData(1, 5);
    for (Entry<Key,Value> entry : createPollutedSource(0, 1).entrySet())
      source.put(entry.getKey(), entry.getValue());

    verify(source, 7, is.getOptions(), GOOD, GOOD, GOOD_VIS, 1);
  }

  @Test
  public void testCommaSeparatedAuthorizations() throws IOException {
    Map<String,String> options = Collections.singletonMap("auths", "x,def,y");

    TreeMap<Key,Value> source = createSourceWithHiddenData(1, 2);
    verify(source, 3, options, GOOD, GOOD, GOOD_VIS, 1);

    source = createSourceWithHiddenData(30, 500);
    verify(source, 530, options, GOOD, GOOD, GOOD_VIS, 30);

    source = createSourceWithHiddenData(1000, 500);
    verify(source, 1500, options, GOOD, GOOD, GOOD_VIS, 1000);
  }

  @Test
  public void testSerializedAuthorizations() throws IOException {
    Map<String,String> options = Collections.singletonMap("auths", new Authorizations("x", "def", "y").serialize());

    TreeMap<Key,Value> source = createSourceWithHiddenData(1, 2);
    verify(source, 3, options, GOOD, GOOD, GOOD_VIS, 1);

    source = createSourceWithHiddenData(30, 500);
    verify(source, 530, options, GOOD, GOOD, GOOD_VIS, 30);

    source = createSourceWithHiddenData(1000, 500);
    verify(source, 1500, options, GOOD, GOOD, GOOD_VIS, 1000);
  }

  @Test
  public void testStaticConfigurators() {
    IteratorSetting is = new IteratorSetting(1, VisibilityFilter.class);
    VisibilityFilter.filterInvalidLabelsOnly(is, false);
    VisibilityFilter.setNegate(is, true);
    VisibilityFilter.setAuthorizations(is, new Authorizations("abc", "def"));

    Map<String,String> opts = is.getOptions();
    assertEquals("false", opts.get("filterInvalid"));
    assertEquals("true", opts.get("negate"));
    assertEquals(new Authorizations("abc", "def").serialize(), opts.get("auths"));
  }

}
