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
package org.apache.accumulo.core.util.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class RangeAdapterTest {

  private static final Gson gson = RangeAdapter.createRangeGson();

  private static class TestRangeWrapper {
    Range range;
  }

  @Test
  public void testRangeNullField() {
    // Verify null Range fields work
    String json = gson.toJson(new TestRangeWrapper());
    TestRangeWrapper testWrapper = gson.fromJson(json, TestRangeWrapper.class);
    assertNull(testWrapper.range);
  }

  @Test
  public void testRangeSerialization() {
    String json = gson.toJson(new Range());
    assertEquals(new Range(), gson.fromJson(json, Range.class));

    // set all fields to verify serialization
    Key key1 = new Key("row1", "cf1", "cq1", 50);
    Key key2 = new Key("row2", "cf2", "cq2", 100);
    key2.setDeleted(true);

    json = gson.toJson(new Range(key1, false, key2, false));
    assertEquals(new Range(key1, false, key2, false), gson.fromJson(json, Range.class));
  }

  @Test
  public void testRangeCollectionSerialization() {
    // Verify a collection can be serialized/deserialized
    String json = gson.toJson(List.of(new Range("1111", "2222"), new Range("abc", "xyz")));
    assertEquals(List.of(new Range("1111", "2222"), new Range("abc", "xyz")),
        gson.fromJson(json, new TypeToken<List<Range>>() {}.getType()));
  }
}
