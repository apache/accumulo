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
package org.apache.accumulo.core.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.security.thrift.TAuthenticationTokenIdentifier;
import org.junit.Before;
import org.junit.Test;

public class ThriftMessageUtilTest {

  private TAuthenticationTokenIdentifier msg;
  private ThriftMessageUtil util;

  @Before
  public void setup() {
    msg = new TAuthenticationTokenIdentifier("principal");
    util = new ThriftMessageUtil();
  }

  @Test
  public void testSerializationAsByteArray() throws IOException {
    ByteBuffer buff = util.serialize(msg);
    TAuthenticationTokenIdentifier copy = new TAuthenticationTokenIdentifier();
    byte[] array = new byte[buff.limit()];
    System.arraycopy(buff.array(), 0, array, 0, buff.limit());
    util.deserialize(array, copy);
    assertEquals(msg, copy);
  }

  @Test
  public void testSerializationAsByteArrayWithLimits() throws IOException {
    ByteBuffer buff = util.serialize(msg);
    TAuthenticationTokenIdentifier copy = new TAuthenticationTokenIdentifier();

    byte[] array = new byte[buff.limit() + 14];
    // Throw some garbage in front and behind the actual message
    array[0] = 'G';
    array[1] = 'A';
    array[2] = 'R';
    array[3] = 'B';
    array[4] = 'A';
    array[5] = 'G';
    array[6] = 'E';
    System.arraycopy(buff.array(), 0, array, 7, buff.limit());
    array[7 + buff.limit()] = 'G';
    array[7 + buff.limit() + 1] = 'A';
    array[7 + buff.limit() + 2] = 'R';
    array[7 + buff.limit() + 3] = 'B';
    array[7 + buff.limit() + 4] = 'A';
    array[7 + buff.limit() + 5] = 'G';
    array[7 + buff.limit() + 6] = 'E';

    util.deserialize(array, 7, buff.limit(), copy);
    assertEquals(msg, copy);
  }
}
