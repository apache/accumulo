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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.junit.Before;
import org.junit.Test;

public class BigDecimalCombinerTest {

  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  private static double delta = 0.00001;

  Encoder<BigDecimal> encoder;
  TreeMap<Key,Value> tm1;
  List<Column> columns;
  Combiner ai;

  @Before
  public void setup() {
    encoder = new BigDecimalCombiner.BigDecimalEncoder();
    tm1 = new TreeMap<Key,Value>();
    columns = Collections.singletonList(new IteratorSetting.Column("cf001"));

    // keys that will aggregate
    CombinerTest.nkv(tm1, 1, 1, 1, 1, false, BigDecimal.valueOf(2), encoder);
    CombinerTest.nkv(tm1, 1, 1, 1, 2, false, BigDecimal.valueOf(2.3), encoder);
    CombinerTest.nkv(tm1, 1, 1, 1, 3, false, BigDecimal.valueOf(-1.4E1), encoder);

    // and keys that will not aggregate
    CombinerTest.nkv(tm1, 1, 2, 1, 1, false, BigDecimal.valueOf(99), encoder);
    CombinerTest.nkv(tm1, 1, 3, 1, 1, false, BigDecimal.valueOf(-88), encoder);
  }

  @Test
  public void testSums() throws IOException {
    ai = new BigDecimalCombiner.BigDecimalSummingCombiner();
    IteratorSetting is = new IteratorSetting(1, BigDecimalCombiner.BigDecimalSummingCombiner.class);
    Combiner.setColumns(is, columns);

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(CombinerTest.nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals(-9.7, encoder.decode(ai.getTopValue().get()).doubleValue(), delta);

    verify();
  }

  @Test
  public void testMin() throws IOException {
    ai = new BigDecimalCombiner.BigDecimalMinCombiner();
    IteratorSetting is = new IteratorSetting(1, BigDecimalCombiner.BigDecimalMinCombiner.class);
    Combiner.setColumns(is, columns);

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(CombinerTest.nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals(-14.0, encoder.decode(ai.getTopValue().get()).doubleValue(), delta);

    verify();
  }

  @Test
  public void testMax() throws IOException {
    ai = new BigDecimalCombiner.BigDecimalMaxCombiner();
    IteratorSetting is = new IteratorSetting(1, BigDecimalCombiner.BigDecimalMaxCombiner.class);
    Combiner.setColumns(is, columns);

    ai.init(new SortedMapIterator(tm1), is.getOptions(), null);
    ai.seek(new Range(), EMPTY_COL_FAMS, false);

    assertTrue(ai.hasTop());
    assertEquals(CombinerTest.nk(1, 1, 1, 3), ai.getTopKey());
    assertEquals(2.3, encoder.decode(ai.getTopValue().get()).doubleValue(), delta);

    verify();
  }

  private void verify() throws IOException {
    ai.next(); // Skip the combined key, since we've already looked at it by now

    // Should have exactly two more keys left over
    assertEquals(CombinerTest.nk(1, 2, 1, 1), ai.getTopKey());
    assertEquals(99.0, encoder.decode(ai.getTopValue().get()).doubleValue(), delta);
    ai.next();

    assertEquals(CombinerTest.nk(1, 3, 1, 1), ai.getTopKey());
    assertEquals(-88.0, encoder.decode(ai.getTopValue().get()).doubleValue(), delta);
    ai.next();

    assertFalse(ai.hasTop());
  }
}
