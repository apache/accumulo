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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;

public class AccumuloConfigurationTest {

  @Test
  public void testGetMemoryInBytes() throws Exception {
    List<Function<String,Long>> funcs = Arrays.asList(AccumuloConfiguration::getFixedMemoryAsBytes, AccumuloConfiguration::getMemoryAsBytes);
    for (Function<String,Long> memFunc : funcs) {
      assertEquals(42l, memFunc.apply("42").longValue());
      assertEquals(42l, memFunc.apply("42b").longValue());
      assertEquals(42l, memFunc.apply("42B").longValue());
      assertEquals(42l * 1024l, memFunc.apply("42K").longValue());
      assertEquals(42l * 1024l, memFunc.apply("42k").longValue());
      assertEquals(42l * 1024l * 1024l, memFunc.apply("42M").longValue());
      assertEquals(42l * 1024l * 1024l, memFunc.apply("42m").longValue());
      assertEquals(42l * 1024l * 1024l * 1024l, memFunc.apply("42G").longValue());
      assertEquals(42l * 1024l * 1024l * 1024l, memFunc.apply("42g").longValue());
    }
    assertEquals(Runtime.getRuntime().maxMemory() / 10, AccumuloConfiguration.getMemoryAsBytes("10%"));
    assertEquals(Runtime.getRuntime().maxMemory() / 5, AccumuloConfiguration.getMemoryAsBytes("20%"));
  }

  @Test
  public void testGetTimeInMillis() {
    assertEquals(42L * 24 * 60 * 60 * 1000, AccumuloConfiguration.getTimeInMillis("42d"));
    assertEquals(42L * 60 * 60 * 1000, AccumuloConfiguration.getTimeInMillis("42h"));
    assertEquals(42L * 60 * 1000, AccumuloConfiguration.getTimeInMillis("42m"));
    assertEquals(42L * 1000, AccumuloConfiguration.getTimeInMillis("42s"));
    assertEquals(42L * 1000, AccumuloConfiguration.getTimeInMillis("42"));
    assertEquals(42L, AccumuloConfiguration.getTimeInMillis("42ms"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFixedMemoryAsBytesFailureCases1() throws Exception {
    AccumuloConfiguration.getFixedMemoryAsBytes("42x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFixedMemoryAsBytesFailureCases2() throws Exception {
    AccumuloConfiguration.getFixedMemoryAsBytes("FooBar");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFixedMemoryAsBytesFailureCases3() throws Exception {
    AccumuloConfiguration.getFixedMemoryAsBytes("40%");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMemoryAsBytesFailureCases1() throws Exception {
    AccumuloConfiguration.getMemoryAsBytes("42x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMemoryAsBytesFailureCases2() throws Exception {
    AccumuloConfiguration.getMemoryAsBytes("FooBar");
  }

  @Test
  public void testGetPropertyByString() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    boolean found = false;
    for (Property p : Property.values()) {
      if (p.getType() != PropertyType.PREFIX) {
        found = true;
        // ensure checking by property and by key works the same
        assertEquals(c.get(p), c.get(p.getKey()));
        // ensure that getting by key returns the expected value
        assertEquals(p.getDefaultValue(), c.get(p.getKey()));
      }
    }
    assertTrue("test was a dud, and did nothing", found);
  }

  @Test
  public void testGetSinglePort() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "9997");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(1, ports.length);
    assertEquals(9997, ports[0]);
  }

  @Test
  public void testGetAnyPort() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "0");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(1, ports.length);
    assertEquals(0, ports[0]);
  }

  @Test
  public void testGetInvalidPort() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "1020");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(1, ports.length);
    assertEquals(Integer.parseInt(Property.TSERV_CLIENTPORT.getDefaultValue()), ports[0]);
  }

  @Test
  public void testGetPortRange() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "9997-9999");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(3, ports.length);
    assertEquals(9997, ports[0]);
    assertEquals(9998, ports[1]);
    assertEquals(9999, ports[2]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPortRangeInvalidLow() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "1020-1026");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(3, ports.length);
    assertEquals(1024, ports[0]);
    assertEquals(1025, ports[1]);
    assertEquals(1026, ports[2]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPortRangeInvalidHigh() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "65533-65538");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(3, ports.length);
    assertEquals(65533, ports[0]);
    assertEquals(65534, ports[1]);
    assertEquals(65535, ports[2]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetPortInvalidSyntax() {
    AccumuloConfiguration c = AccumuloConfiguration.getDefaultConfiguration();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "[65533,65538]");
    cc.getPort(Property.TSERV_CLIENTPORT);
  }

}
