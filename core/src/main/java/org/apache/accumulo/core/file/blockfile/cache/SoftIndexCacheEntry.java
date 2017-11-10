/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class SoftIndexCacheEntry implements CacheEntry {

  private final byte[] data;
  private final AtomicReference<SoftReference<Object>> indexRef;

  public SoftIndexCacheEntry(byte[] data) {
    this.data = data;
    this.indexRef = new AtomicReference<SoftReference<Object>>();
  }

  @Override
  public byte[] getBuffer() {
    return data;
  }

  /**
   * Stores the result of the passed in supplier using a {@link SoftReference}
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T> T getIndex(Supplier<T> supplier) {

    T ret = null;

    do {
      SoftReference<Object> softRef = indexRef.get();

      if (softRef != null) {
        ret = (T) softRef.get();
      }

      if (ret == null) {
        Object tmp = supplier.get();
        SoftReference<Object> softRef2 = new SoftReference<Object>(tmp);
        if (indexRef.compareAndSet(softRef, softRef2)) {
          ret = (T) tmp;
        }
      }
    } while (ret == null);

    return ret;
  }
}
