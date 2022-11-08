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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

/**
 * Test the TextUtil class.
 */
public class TextUtilTest {

  @Test
  public void testGetBytes() {
    String longMessage = "This is some text";
    Text longMessageText = new Text(longMessage);
    String smallerMessage = "a";
    Text smallerMessageText = new Text(smallerMessage);
    Text someText = new Text(longMessage);
    assertEquals(someText, longMessageText);
    someText.set(smallerMessageText);
    assertTrue(someText.getLength() != someText.getBytes().length);
    assertEquals(TextUtil.getBytes(someText).length, smallerMessage.length());
    assertEquals((new Text(TextUtil.getBytes(someText))), smallerMessageText);
  }

}
