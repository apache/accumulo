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
package org.apache.accumulo.access;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.LinkedHashMap;
import java.util.Map;

class CachingAccessEvaluator implements AccessEvaluator {

  private final AccessEvaluator accessEvaluator;
  private final LinkedHashMap<String,Boolean> cache;

  CachingAccessEvaluator(AccessEvaluator accessEvaluator, int cacheSize) {
    if (cacheSize <= 0) {
      throw new IllegalArgumentException();
    }
    this.accessEvaluator = accessEvaluator;
    this.cache = new LinkedHashMap<>(cacheSize, 0.75f, true) {
      @Override
      public boolean removeEldestEntry(Map.Entry<String,Boolean> entry) {
        return size() > cacheSize;
      }
    };
  }

  @Override
  public boolean canAccess(String expression) throws IllegalArgumentException {
    return cache.computeIfAbsent(expression, accessEvaluator::canAccess);
  }

  @Override
  public boolean canAccess(byte[] expression) throws IllegalArgumentException {
    // TODO avoid converting to string, maybe create separate cache for byte arrays keys
    return canAccess(new String(expression, UTF_8));
  }

  @Override
  public boolean canAccess(AccessExpression expression) throws IllegalArgumentException {
    return cache.computeIfAbsent(expression.getExpression(),
        k -> accessEvaluator.canAccess(expression));
  }
}
