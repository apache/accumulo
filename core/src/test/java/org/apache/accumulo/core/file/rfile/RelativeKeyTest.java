/**
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
package org.apache.accumulo.core.file.rfile;

import junit.framework.TestCase;

/**
 * 
 */
public class RelativeKeyTest extends TestCase {
  public void test1() {
    assertEquals(1, RelativeKey.nextArraySize(0));
    assertEquals(1, RelativeKey.nextArraySize(1));
    assertEquals(2, RelativeKey.nextArraySize(2));
    assertEquals(4, RelativeKey.nextArraySize(3));
    assertEquals(4, RelativeKey.nextArraySize(4));
    assertEquals(8, RelativeKey.nextArraySize(5));
    assertEquals(8, RelativeKey.nextArraySize(8));
    assertEquals(16, RelativeKey.nextArraySize(9));
    
    assertEquals(1 << 16, RelativeKey.nextArraySize((1 << 16) - 1));
    assertEquals(1 << 16, RelativeKey.nextArraySize(1 << 16));
    assertEquals(1 << 17, RelativeKey.nextArraySize((1 << 16) + 1));
    
    assertEquals(1 << 30, RelativeKey.nextArraySize((1 << 30) - 1));

    assertEquals(1 << 30, RelativeKey.nextArraySize(1 << 30));

    assertEquals(Integer.MAX_VALUE, RelativeKey.nextArraySize(Integer.MAX_VALUE - 1));
    assertEquals(Integer.MAX_VALUE, RelativeKey.nextArraySize(Integer.MAX_VALUE));
  }
  
}
