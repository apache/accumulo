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
package org.apache.accumulo.core.iteratorsImpl;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;

import com.google.common.base.Preconditions;

/**
 * Utility for parsing a single iterator key/value property.
 */
public class IteratorProperty {

  private final IteratorUtil.IteratorScope scope;
  private final String name;

  private final int priority;
  private final String className;

  private final String optionKey;
  private final String optionValue;

  private IteratorProperty(String iterName, IteratorUtil.IteratorScope scope, int priority,
      String className) {
    this.name = iterName;
    this.scope = scope;
    this.priority = priority;
    this.className = className;
    this.optionKey = null;
    this.optionValue = null;
  }

  private IteratorProperty(String iterName, IteratorUtil.IteratorScope scope, String optionName,
      String optionValue) {
    this.name = iterName;
    this.scope = scope;
    this.priority = -1;
    this.className = null;
    this.optionKey = optionName;
    this.optionValue = optionValue;
  }

  public boolean isOption() {
    return optionKey != null;
  }

  public String getClassName() {
    Preconditions.checkState(!isOption());
    return className;
  }

  public String getName() {
    return name;
  }

  public String getOptionKey() {
    Preconditions.checkState(isOption());
    return optionKey;
  }

  public String getOptionValue() {
    Preconditions.checkState(isOption());
    return optionValue;
  }

  public int getPriority() {
    Preconditions.checkState(!isOption());
    return priority;
  }

  public IteratorUtil.IteratorScope getScope() {
    return scope;
  }

  /**
   * Creates an initial iterator setting without options.
   *
   * @throws IllegalStateException if {@link #isOption()} returns true
   */
  public IteratorSetting toSetting() {
    Preconditions.checkState(!isOption());
    return new IteratorSetting(getPriority(), getName(), getClassName());
  }

  private static void check(boolean b, String property, String value) {
    if (!b) {
      throw new IllegalArgumentException("Illegal iterator property: " + property + "=" + value);
    }
  }

  /**
   * Parses an iterator key value property.
   *
   * @return parsed iterator property or null if the property does not start with the iterator
   *         property prefix.
   * @throws IllegalArgumentException if the iterator property is malformed.
   */
  public static IteratorProperty parse(String property, String value) {
    if (!property.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())) {
      return null;
    }

    String[] iterPropParts = property.split("\\.");
    check(iterPropParts.length >= 4, property, value);
    IteratorUtil.IteratorScope scope = IteratorUtil.IteratorScope.valueOf(iterPropParts[2]);
    String iterName = iterPropParts[3];

    if (iterPropParts.length == 4) {
      String[] valTokens = value.split(",");
      check(valTokens.length == 2, property, value);
      return new IteratorProperty(iterName, scope, Integer.parseInt(valTokens[0]), valTokens[1]);
    } else if (iterPropParts.length == 6) {
      check(iterPropParts[4].equals("opt"), property, value);
      return new IteratorProperty(iterName, scope, iterPropParts[5], value);
    } else {
      throw new IllegalArgumentException("Illegal iterator property: " + property + "=" + value);
    }
  }
}
