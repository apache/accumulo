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
package org.apache.accumulo.server.problems;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProblemReportingIteratorTest {
  private static final TableId TABLE_ID = TableId.of("table");
  private static final String RESOURCE = "resource";

  private InterruptibleIterator ii;
  private ProblemReportingIterator pri;

  @BeforeEach
  public void setUp() {
    ii = EasyMock.createMock(InterruptibleIterator.class);
    pri = new ProblemReportingIterator(null, TABLE_ID, RESOURCE, false, ii);
  }

  @Test
  public void testBasicGetters() {
    Key key = EasyMock.createMock(Key.class);
    expect(ii.getTopKey()).andReturn(key);
    Value value = EasyMock.createMock(Value.class);
    expect(ii.getTopValue()).andReturn(value);
    expect(ii.hasTop()).andReturn(true);
    replay(ii);
    assertSame(key, pri.getTopKey());
    assertSame(value, pri.getTopValue());
    assertTrue(pri.hasTop());
    assertFalse(pri.sawError());
    assertEquals(RESOURCE, pri.getResource());
  }

  @Test
  public void testInit() {
    assertThrows(UnsupportedOperationException.class, () -> pri.init(null, null, null));
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
    Range r = EasyMock.createMock(Range.class);
    Collection<ByteSequence> f = new java.util.HashSet<>();
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
