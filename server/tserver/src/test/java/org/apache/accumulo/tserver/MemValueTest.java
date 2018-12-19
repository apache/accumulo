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
package org.apache.accumulo.tserver;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import org.apache.accumulo.core.data.Value;
import org.junit.Test;

public class MemValueTest {

  @Test
  public void testDecodeDoesntModifyInputValue() {
    Value v = new Value("2.0".getBytes(Charset.forName("UTF-8")));
    Value encodedValue = MemValue.encode(v, 3);
    MemValue m1 = MemValue.decode(encodedValue);
    MemValue m2 = MemValue.decode(encodedValue);
    assertEquals(m1.kvCount, m2.kvCount);
  }
}
