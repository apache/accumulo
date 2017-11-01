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
package org.apache.accumulo.core.conf;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class CliConfigurationTest {

  @Test
  public void testBasic() {
    try {
      CliConfiguration.set(null);
      Assert.fail();
    } catch (NullPointerException e) {
      // expected
    }
    CliConfiguration.set(new HashMap<>());

    Map<String,String> expected = new HashMap<>();
    expected.put(Property.TRACE_USER.getKey(), "test");
    expected.put(Property.TSERV_CLIENTPORT.getKey(), "123");
    CliConfiguration.set(expected);

    Assert.assertEquals("test", CliConfiguration.get(Property.TRACE_USER));

    Map<String,String> results = new HashMap<>();
    CliConfiguration.getProperties(results, p -> p.startsWith("trace"));
    Assert.assertEquals(ImmutableMap.of(Property.TRACE_USER.getKey(), "test"), results);
  }
}
