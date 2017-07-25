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
package org.apache.accumulo.core.util.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.junit.Test;

public class DateFormatSupplierTest {

  /** Asserts two supplier instance create independent objects */
  private void assertSuppliersIndependent(ThreadLocal<DateFormat> supplierA, ThreadLocal<DateFormat> supplierB) {
    DateFormat getA1 = supplierA.get();
    DateFormat getA2 = supplierA.get();
    assertSame(getA1, getA2);

    DateFormat getB1 = supplierB.get();
    DateFormat getB2 = supplierB.get();

    assertSame(getB1, getB2);
    assertNotSame(getA1, getB1);
  }

  @Test
  public void testCreateDefaultFormatSupplier() throws Exception {
    ThreadLocal<DateFormat> supplierA = DateFormatSupplier.createDefaultFormatSupplier();
    ThreadLocal<DateFormat> supplierB = DateFormatSupplier.createDefaultFormatSupplier();
    assertSuppliersIndependent(supplierA, supplierB);
  }

  @Test
  public void testCreateSimpleFormatSupplier() throws Exception {
    final String format = DateFormatSupplier.HUMAN_READABLE_FORMAT;
    DateFormatSupplier supplierA = DateFormatSupplier.createSimpleFormatSupplier(format);
    DateFormatSupplier supplierB = DateFormatSupplier.createSimpleFormatSupplier(format);
    assertSuppliersIndependent(supplierA, supplierB);

    // since dfA and dfB come from different suppliers, altering the TimeZone on one does not affect the other
    supplierA.setTimeZone(TimeZone.getTimeZone("UTC"));
    final DateFormat dfA = supplierA.get();

    supplierB.setTimeZone(TimeZone.getTimeZone("EST"));
    final DateFormat dfB = supplierB.get();

    final String resultA = dfA.format(new Date(0));
    assertEquals("1970/01/01 00:00:00.000", resultA);

    final String resultB = dfB.format(new Date(0));
    assertEquals("1969/12/31 19:00:00.000", resultB);

    assertTrue(!resultA.equals(resultB));

  }
}
