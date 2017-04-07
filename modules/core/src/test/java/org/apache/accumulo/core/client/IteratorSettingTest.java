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
package org.apache.accumulo.core.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.DevNull;
import org.junit.Test;

/**
 * Test cases for the IteratorSetting class
 */
public class IteratorSettingTest {

  IteratorSetting setting1 = new IteratorSetting(500, "combiner", Combiner.class.getName());
  IteratorSetting setting2 = new IteratorSetting(500, "combiner", Combiner.class.getName());
  IteratorSetting setting3 = new IteratorSetting(500, "combiner", Combiner.class.getName());
  IteratorSetting devnull = new IteratorSetting(500, "devNull", DevNull.class.getName());
  final IteratorSetting nullsetting = null;
  IteratorSetting setting4 = new IteratorSetting(300, "combiner", Combiner.class.getName());
  IteratorSetting setting5 = new IteratorSetting(500, "foocombiner", Combiner.class.getName());
  IteratorSetting setting6 = new IteratorSetting(500, "combiner", "MySuperCombiner");

  @Test
  public final void testHashCodeSameObject() {
    assertEquals(setting1.hashCode(), setting1.hashCode());
  }

  @Test
  public final void testHashCodeEqualObjects() {
    assertEquals(setting1.hashCode(), setting2.hashCode());
  }

  @Test
  public final void testEqualsObjectReflexive() {
    assertEquals(setting1, setting1);
  }

  @Test
  public final void testEqualsObjectSymmetric() {
    assertEquals(setting1, setting2);
    assertEquals(setting2, setting1);
  }

  @Test
  public final void testEqualsObjectTransitive() {
    assertEquals(setting1, setting2);
    assertEquals(setting2, setting3);
    assertEquals(setting1, setting3);
  }

  @Test
  public final void testEqualsNullSetting() {
    assertNotEquals(setting1, nullsetting);
  }

  @Test
  public final void testEqualsObjectNotEqual() {
    assertNotEquals(setting1, devnull);
  }

  @Test
  public final void testEqualsObjectProperties() {
    IteratorSetting mysettings = new IteratorSetting(500, "combiner", Combiner.class.getName());
    assertEquals(setting1, mysettings);
    mysettings.addOption("myoption1", "myvalue1");
    assertNotEquals(setting1, mysettings);
  }

  @Test
  public final void testEqualsDifferentMembers() {
    assertNotEquals(setting1, setting4);
    assertNotEquals(setting1, setting5);
    assertNotEquals(setting1, setting6);
  }

  @Test
  public void testEquivalentConstructor() {
    IteratorSetting setting1 = new IteratorSetting(100, Combiner.class);
    IteratorSetting setting2 = new IteratorSetting(100, "Combiner", Combiner.class, new HashMap<String,String>());

    assertEquals(setting1, setting2);

    IteratorSetting notEqual1 = new IteratorSetting(100, "FooCombiner", Combiner.class, new HashMap<String,String>());

    assertNotEquals(setting1, notEqual1);

    Map<String,String> props = new HashMap<>();
    props.put("foo", "bar");
    IteratorSetting notEquals2 = new IteratorSetting(100, "Combiner", Combiner.class, props);

    assertNotEquals(setting1, notEquals2);
  }
}
