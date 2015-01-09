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

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class DefaultFormatterTest {

  DefaultFormatter df;
  Iterable<Entry<Key,Value>> empty = Collections.<Key,Value> emptyMap().entrySet();

  @Before
  public void setUp() {
    df = new DefaultFormatter();
  }

  @Test(expected = IllegalStateException.class)
  public void testDoubleInitialize() {
    df.initialize(empty, true);
    df.initialize(empty, true);
  }

  @Test(expected = IllegalStateException.class)
  public void testNextBeforeInitialize() {
    df.hasNext();
  }

  @Test
  public void testAppendBytes() {
    StringBuilder sb = new StringBuilder();
    byte[] data = new byte[] {0, '\\', 'x', -0x01};

    DefaultFormatter.appendValue(sb, new Value());
    assertEquals("", sb.toString());

    DefaultFormatter.appendText(sb, new Text(data));
    assertEquals("\\x00\\\\x\\xFF", sb.toString());
  }
}
