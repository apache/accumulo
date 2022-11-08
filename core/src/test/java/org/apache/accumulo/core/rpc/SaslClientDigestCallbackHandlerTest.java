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
package org.apache.accumulo.core.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SaslClientDigestCallbackHandlerTest {

  @Test
  public void testEquality() {
    SaslClientDigestCallbackHandler handler1 =
        new SaslClientDigestCallbackHandler("user", "mypass".toCharArray()),
        handler2 = new SaslClientDigestCallbackHandler("user", "mypass".toCharArray());
    assertEquals(handler1, handler2);
    assertEquals(handler1.hashCode(), handler2.hashCode());
  }

}
