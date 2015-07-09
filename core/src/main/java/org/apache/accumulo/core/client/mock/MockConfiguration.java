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
package org.apache.accumulo.core.client.mock;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

import com.google.common.base.Predicate;

/**
 * @deprecated since 1.8.0; use MiniAccumuloCluster or a standard mock framework instead.
 */
@Deprecated
class MockConfiguration extends AccumuloConfiguration {
  Map<String,String> map;

  MockConfiguration(Map<String,String> settings) {
    map = settings;
  }

  public void put(String k, String v) {
    map.put(k, v);
  }

  @Override
  public String get(Property property) {
    return map.get(property.getKey());
  }

  /**
   * Don't use this method. It has been deprecated. Its parameters are not public API and subject to change.
   *
   * @deprecated since 1.7.0; use {@link #getProperties(Map, Predicate)} instead.
   */
  @Deprecated
  public void getProperties(Map<String,String> props, final PropertyFilter filter) {
    // convert PropertyFilter to Predicate
    getProperties(props, new Predicate<String>() {

      @Override
      public boolean apply(String input) {
        return filter.accept(input);
      }
    });
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    for (Entry<String,String> entry : map.entrySet()) {
      if (filter.apply(entry.getKey())) {
        props.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
