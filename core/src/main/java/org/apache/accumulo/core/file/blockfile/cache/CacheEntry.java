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
package org.apache.accumulo.core.file.blockfile.cache;

import java.util.function.Supplier;

public interface CacheEntry {

  public static interface Weighbable {
    int weight();
  }

  byte[] getBuffer();

  /**
   * Optionally cache what is returned by the supplier along with this cache entry. If caching what is returned by the supplier is not supported, its ok to
   * return null.
   *
   * <p>
   * This method exists to support building indexes of frequently accessed cached data.
   */
  <T extends Weighbable> T getIndex(Supplier<T> supplier);

  /**
   * The object optionally stored by {@link #getIndex(Supplier)} is a mutable object. Accumulo will call this method whenever the weight of that object changes.
   */
  void indexWeightChanged();
}
