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
package org.apache.accumulo.core.client.lexicoder;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.junit.jupiter.api.Test;

public class ReverseLexicoderTest extends AbstractLexicoderTest {

  @Test
  public void testSortOrder() {
    Comparator<Long> comp = Collections.reverseOrder();
    assertSortOrder(new ReverseLexicoder<>(new LongLexicoder()), comp,
        Arrays.asList(Long.MIN_VALUE, 0xff1234567890abcdL, 0xffff1234567890abL, 0xffffff567890abcdL,
            0xffffffff7890abcdL, 0xffffffffff90abcdL, 0xffffffffffffabcdL, 0xffffffffffffffcdL, -1L,
            0L, 0x01L, 0x1234L, 0x123456L, 0x12345678L, 0x1234567890L, 0x1234567890abL,
            0x1234567890abcdL, 0x1234567890abcdefL, Long.MAX_VALUE));

    Comparator<String> comp2 = Collections.reverseOrder();
    assertSortOrder(new ReverseLexicoder<>(new StringLexicoder()), comp2,
        Arrays.asList("a", "aa", "ab", "b", "aab"));

  }

  /**
   * Just a simple test verifying reverse indexed dates
   */
  @Test
  public void testReverseSortDates() {

    ReverseLexicoder<Date> revLex = new ReverseLexicoder<>(new DateLexicoder());

    Calendar cal = Calendar.getInstance();
    cal.set(1920, 1, 2, 3, 4, 5); // create an instance prior to 1970 for ACCUMULO-3385
    Date date0 = new Date(cal.getTimeInMillis());
    Date date1 = new Date();
    Date date2 = new Date(System.currentTimeMillis() + 10000);
    Date date3 = new Date(System.currentTimeMillis() + 500);

    Comparator<Date> comparator = Collections.reverseOrder();
    assertSortOrder(revLex, comparator, Arrays.asList(date0, date1, date2, date3));

    // truncate date to hours
    long time = System.currentTimeMillis() - (System.currentTimeMillis() % 3600000);
    Date date = new Date(time);

    System.out.println(date);

  }

  @Test
  public void testDecodes() {
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), Long.MIN_VALUE);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), -1L);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), 0L);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), 1L);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), Long.MAX_VALUE);
  }
}
