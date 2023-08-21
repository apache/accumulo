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
package org.apache.accumulo.visibility;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.LinkedHashMap;
import java.util.Map;

class CachingVisibilityArbiter implements VisibilityArbiter {

  private final VisibilityArbiter visibilityArbiter;
  private final LinkedHashMap<String,Boolean> cache;

  CachingVisibilityArbiter(VisibilityArbiter visibilityArbiter, int cacheSize) {
    if (cacheSize <= 0) {
      throw new IllegalArgumentException();
    }
    this.visibilityArbiter = visibilityArbiter;
    this.cache = new LinkedHashMap<>(cacheSize, 0.75f, true) {
      @Override
      public boolean removeEldestEntry(Map.Entry<String,Boolean> entry) {
        return size() > cacheSize;
      }
    };
  }

  @Override
  public boolean isVisible(String expression) throws IllegalArgumentException {
    return cache.computeIfAbsent(expression, visibilityArbiter::isVisible);
  }

  @Override
  public boolean isVisible(byte[] expression) throws IllegalArgumentException {
    // TODO avoid converting to string, maybe create separate cache for byte arrays keys
    return isVisible(new String(expression, UTF_8));
  }

  @Override
  public boolean isVisible(VisibilityExpression expression) throws IllegalArgumentException {
    return cache.computeIfAbsent(expression.getExpression(),
        k -> visibilityArbiter.isVisible(expression));
  }
}
