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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class ObservableConfigurationTest {
  private static class TestObservableConfig extends ObservableConfiguration {
    @Override
    public String get(Property property) {
      return null;
    }

    @Override
    public void getProperties(Map<String,String> props, PropertyFilter filter) {}
  }

  private ObservableConfiguration c;
  private ConfigurationObserver co1;

  @Before
  public void setUp() {
    c = new TestObservableConfig();
    co1 = createMock(ConfigurationObserver.class);
  }

  @Test
  public void testAddAndRemove() {
    ConfigurationObserver co2 = createMock(ConfigurationObserver.class);
    c.addObserver(co1);
    c.addObserver(co2);
    Collection<ConfigurationObserver> cos = c.getObservers();
    assertEquals(2, cos.size());
    assertTrue(cos.contains(co1));
    assertTrue(cos.contains(co2));
    c.removeObserver(co1);
    cos = c.getObservers();
    assertEquals(1, cos.size());
    assertTrue(cos.contains(co2));
  }

  @Test(expected = NullPointerException.class)
  public void testNoNullAdd() {
    c.addObserver(null);
  }

  @Test
  public void testSessionExpired() {
    c.addObserver(co1);
    co1.sessionExpired();
    replay(co1);
    c.expireAllObservers();
    verify(co1);
  }

  @Test
  public void testPropertyChanged() {
    String key = "key";
    c.addObserver(co1);
    co1.propertyChanged(key);
    replay(co1);
    c.propertyChanged(key);
    verify(co1);
  }

  @Test
  public void testPropertiesChanged() {
    c.addObserver(co1);
    co1.propertiesChanged();
    replay(co1);
    c.propertiesChanged();
    verify(co1);
  }
}
