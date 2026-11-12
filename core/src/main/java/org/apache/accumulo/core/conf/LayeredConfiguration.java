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
package org.apache.accumulo.core.conf;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Layers one accumulo configuration on top of another. When getting properties will look in the
 * outer layer first and if not present will fall back to the inner layer.
 */
public class LayeredConfiguration extends AccumuloConfiguration {

  private final AccumuloConfiguration inner;
  private final AccumuloConfiguration outer;

  public LayeredConfiguration(AccumuloConfiguration inner, AccumuloConfiguration outer) {
    this.inner = Objects.requireNonNull(inner);
    this.outer = Objects.requireNonNull(outer);
  }

  @Override
  public String get(Property property) {
    String value = outer.get(property);
    if (value == null) {
      value = inner.get(property);
    }
    return value;
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    inner.getProperties(props, filter);
    outer.getProperties(props, filter);
  }

  @Override
  public boolean isPropertySet(Property prop) {
    return outer.isPropertySet(prop) || inner.isPropertySet(prop);
  }

  @Override
  public long getUpdateCount() {
    return outer.getUpdateCount() + inner.getUpdateCount();
  }

  @Override
  public void invalidateCache() {
    outer.invalidateCache();
    inner.invalidateCache();
  }
}
