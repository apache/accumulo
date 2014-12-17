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
package org.apache.accumulo.server.util;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.server.util.DefaultMap;
import org.junit.Test;

public class DefaultMapTest {
  
  @Test
  public void testDefaultMap() {
    Integer value = new DefaultMap<String,Integer>(0).get("test");
    assertNotNull(value);
    assertEquals(new Integer(0), value);
    value = new DefaultMap<String,Integer>(1).get("test");
    assertNotNull(value);
    assertEquals(new Integer(1), value);
    
    AtomicInteger canConstruct = new DefaultMap<String,AtomicInteger>(new AtomicInteger(1)).get("test");
    assertNotNull(canConstruct);
    assertEquals(new AtomicInteger(0).get(), canConstruct.get());
    
    DefaultMap<String,String> map = new DefaultMap<String,String>("");
    assertEquals(map.get("foo"), "");
    map.put("foo", "bar");
    assertEquals(map.get("foo"), "bar");
  }
  
}
