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
package org.apache.accumulo.test.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class JsonMetricsValuesTest {

  JsonMetricsValues v1;

  @Before
  public void init() {
    v1 = new JsonMetricsValues();
    v1.setTimestamp(System.currentTimeMillis());
    v1.addMetric("a", "1");
    v1.addMetric("b", "2");
    v1.sign();
  }

  @Test
  public void roundTrip() {

    String json = v1.toJson();

    JsonMetricsValues result = JsonMetricsValues.fromJson(json);

    assertEquals(v1.getTimestamp(), result.getTimestamp());
    assertEquals(v1.getMetrics(), result.getMetrics());
    assertEquals(v1.getSignature(), result.getSignature());
  }

  @Test
  public void signing() {

    String signature = v1.getSignature();

    assertTrue(v1.verify(signature));

    v1.setTimestamp(System.currentTimeMillis());
    assertFalse(v1.verify(signature));

  }
}
