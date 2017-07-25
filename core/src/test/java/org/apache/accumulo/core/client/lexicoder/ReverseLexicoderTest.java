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
package org.apache.accumulo.core.client.lexicoder;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoderTest;
import org.junit.Test;

public class ReverseLexicoderTest extends AbstractLexicoderTest {
  public void testSortOrder() {
    Comparator<Long> comp = Collections.reverseOrder();
    assertSortOrder(new ReverseLexicoder<>(new LongLexicoder()), comp, Arrays.asList(Long.MIN_VALUE, 0xff1234567890abcdl, 0xffff1234567890abl,
        0xffffff567890abcdl, 0xffffffff7890abcdl, 0xffffffffff90abcdl, 0xffffffffffffabcdl, 0xffffffffffffffcdl, -1l, 0l, 0x01l, 0x1234l, 0x123456l,
        0x12345678l, 0x1234567890l, 0x1234567890abl, 0x1234567890abcdl, 0x1234567890abcdefl, Long.MAX_VALUE));

    Comparator<String> comp2 = Collections.reverseOrder();
    assertSortOrder(new ReverseLexicoder<>(new StringLexicoder()), comp2, Arrays.asList("a", "aa", "ab", "b", "aab"));

  }

  /**
   * Just a simple test verifying reverse indexed dates
   */
  @Test
  public void testReverseSortDates() throws UnsupportedEncodingException {

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

  public void testDecodes() {
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), Long.MIN_VALUE);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), -1l);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), 0l);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), 1l);
    assertDecodes(new ReverseLexicoder<>(new LongLexicoder()), Long.MAX_VALUE);
  }
}
