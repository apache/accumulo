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
package org.apache.accumulo.monitor.util.celltypes;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PreciseNumberTypeTest {

  @Test
  public void test() {
    PreciseNumberType p = new PreciseNumberType(500, 5000, 100, 6000);
    assertEquals("1,000", p.format(1000));
    assertEquals("<span class='error'>1</span>", p.format(1));
    assertEquals("<span class='warning'>5,005</span>", p.format(5005));
    assertEquals("<span class='error'>10,000</span>", p.format(10000));
    assertEquals("-", p.format(null));
    assertEquals("3,000", p.format(Long.valueOf(3000L)));
  }

}
