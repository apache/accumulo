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

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

/**
 * Test the TextUtil class.
 *
 */
public class TextUtilTest extends TestCase {
  /**
   * co
   */
  public void testGetBytes() {
    String longMessage = "This is some text";
    Text longMessageText = new Text(longMessage);
    String smallerMessage = "a";
    Text smallerMessageText = new Text(smallerMessage);
    Text someText = new Text(longMessage);
    assertTrue(someText.equals(longMessageText));
    someText.set(smallerMessageText);
    assertTrue(someText.getLength() != someText.getBytes().length);
    assertTrue(TextUtil.getBytes(someText).length == smallerMessage.length());
    assertTrue((new Text(TextUtil.getBytes(someText))).equals(smallerMessageText));
  }

}
