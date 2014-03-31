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
package org.apache.accumulo.server.problems;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.junit.Before;
import org.junit.Test;

public class ProblemReportingIteratorTest {
  private static final String TABLE = "table";
  private static final String RESOURCE = "resource";

  private InterruptibleIterator ii;
  private ProblemReportingIterator pri;

  @Before
  public void setUp() throws Exception {
    ii = createMock(InterruptibleIterator.class);
    pri = new ProblemReportingIterator(TABLE, RESOURCE, false, ii);
  }

  @Test
  public void testBasicGetters() {
    Key key = createMock(Key.class);
    expect(ii.getTopKey()).andReturn(key);
    Value value = createMock(Value.class);
    expect(ii.getTopValue()).andReturn(value);
    expect(ii.hasTop()).andReturn(true);
    replay(ii);
    assertSame(key, pri.getTopKey());
    assertSame(value, pri.getTopValue());
    assertTrue(pri.hasTop());
    assertFalse(pri.sawError());
    assertEquals(RESOURCE, pri.getResource());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInit() throws Exception {
    pri.init(null, null, null);
  }

  @Test
  public void testNext() throws Exception {
    ii.next();
    replay(ii);
    pri.next();
    verify(ii);
    assertFalse(pri.sawError());
  }

  @Test
  public void testSeek() throws Exception {
    Range r = createMock(Range.class);
    Collection<ByteSequence> f = new java.util.HashSet<ByteSequence>();
    ii.seek(r, f, true);
    replay(ii);
    pri.seek(r, f, true);
    verify(ii);
    assertFalse(pri.sawError());
  }

  @Test
  public void testSetInterruptFlag() {
    AtomicBoolean flag = new AtomicBoolean(true);
    ii.setInterruptFlag(flag);
    replay(ii);
    pri.setInterruptFlag(flag);
    verify(ii);
  }
}
