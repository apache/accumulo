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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

public class AccumuloConfigurationTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetPropertyByString() {
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
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
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "9997");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(1, ports.length);
    assertEquals(9997, ports[0]);
  }

  @Test
  public void testGetAnyPort() {
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "0");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(1, ports.length);
    assertEquals(0, ports[0]);
  }

  @Test
  public void testGetInvalidPort() {
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "1020");
    int[] ports = cc.getPort(Property.TSERV_CLIENTPORT);
    assertEquals(1, ports.length);
    assertEquals(Integer.parseInt(Property.TSERV_CLIENTPORT.getDefaultValue()), ports[0]);
  }

  @Test
  public void testGetPortRange() {
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
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
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
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
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
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
    AccumuloConfiguration c = DefaultConfiguration.getInstance();
    ConfigurationCopy cc = new ConfigurationCopy(c);
    cc.set(Property.TSERV_CLIENTPORT, "[65533,65538]");
    cc.getPort(Property.TSERV_CLIENTPORT);
  }

  private static class TestConfiguration extends AccumuloConfiguration {

    private HashMap<String,String> props = new HashMap<>();
    private int upCount = 0;

    public void set(String p, String v) {
      props.put(p, v);
      upCount++;
    }

    @Override
    public long getUpdateCount() {
      return upCount;
    }

    @Override
    public String get(Property property) {
      return props.get(property.getKey());
    }

    @Override
    public void getProperties(Map<String,String> output, Predicate<String> filter) {
      for (Entry<String,String> entry : props.entrySet()) {
        if (filter.test(entry.getKey())) {
          output.put(entry.getKey(), entry.getValue());
        }
      }
    }

  }

  @Test
  public void testMutatePrefixMap() {
    TestConfiguration tc = new TestConfiguration();
    tc.set(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a1", "325");
    tc.set(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a2", "asg34");

    Map<String,String> pm1 = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);

    Map<String,String> expected1 = new HashMap<>();
    expected1.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a1", "325");
    expected1.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a2", "asg34");
    assertEquals(expected1, pm1);

    thrown.expect(UnsupportedOperationException.class);
    pm1.put("k9", "v3");
  }

  @Test
  public void testGetByPrefix() {
    // This test checks that when anything changes that all prefix maps are regenerated. However when there are not changes the test expects all the exact same
    // map to always be returned.

    TestConfiguration tc = new TestConfiguration();

    tc.set(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a1", "325");
    tc.set(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a2", "asg34");

    tc.set(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i1", "class34");
    tc.set(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i1.opt", "o99");

    Map<String,String> pm1 = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    Map<String,String> pm2 = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);

    assertSame(pm1, pm2);
    Map<String,String> expected1 = new HashMap<>();
    expected1.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a1", "325");
    expected1.put(Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "a2", "asg34");
    assertEquals(expected1, pm1);

    Map<String,String> pm3 = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);
    Map<String,String> pm4 = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);

    assertSame(pm3, pm4);
    Map<String,String> expected2 = new HashMap<>();
    expected2.put(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i1", "class34");
    expected2.put(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i1.opt", "o99");
    assertEquals(expected2, pm3);

    Map<String,String> pm5 = tc.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    Map<String,String> pm6 = tc.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    assertSame(pm5, pm6);
    assertEquals(0, pm5.size());

    // ensure getting one prefix does not cause others to unnecessarily regenerate
    Map<String,String> pm7 = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    assertSame(pm1, pm7);

    Map<String,String> pm8 = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);
    assertSame(pm3, pm8);

    Map<String,String> pm9 = tc.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    assertSame(pm5, pm9);

    tc.set(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i2", "class42");
    tc.set(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i2.opt", "o78234");

    expected2.put(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i2", "class42");
    expected2.put(Property.TABLE_ITERATOR_SCAN_PREFIX.getKey() + "i2.opt", "o78234");

    Map<String,String> pmA = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);
    Map<String,String> pmB = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);
    assertNotSame(pm3, pmA);
    assertSame(pmA, pmB);
    assertEquals(expected2, pmA);

    Map<String,String> pmC = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    Map<String,String> pmD = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    assertNotSame(pm1, pmC);
    assertSame(pmC, pmD);
    assertEquals(expected1, pmC);

    tc.set(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "ctx123", "hdfs://ib/p1");

    Map<String,String> pmE = tc.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    Map<String,String> pmF = tc.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    assertSame(pmE, pmF);
    assertNotSame(pm5, pmE);
    assertEquals(ImmutableMap.of(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "ctx123", "hdfs://ib/p1"), pmE);

    Map<String,String> pmG = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);
    Map<String,String> pmH = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);
    assertNotSame(pmA, pmG);
    assertSame(pmG, pmH);
    assertEquals(expected2, pmG);

    Map<String,String> pmI = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    Map<String,String> pmJ = tc.getAllPropertiesWithPrefix(Property.TABLE_ARBITRARY_PROP_PREFIX);
    assertNotSame(pmC, pmI);
    assertSame(pmI, pmJ);
    assertEquals(expected1, pmI);

    Map<String,String> pmK = tc.getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY);
    assertSame(pmE, pmK);

    Map<String,String> pmL = tc.getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_SCAN_PREFIX);
    assertSame(pmG, pmL);
  }
}
