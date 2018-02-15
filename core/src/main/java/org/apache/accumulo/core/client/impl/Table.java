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
package org.apache.accumulo.core.client.impl;

import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.Instance;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class Table {

  /**
   * Object representing an internal table ID. This class was created to help with type safety. For help obtaining the value of a table ID from Zookeeper, see
   * {@link Tables#getTableId(Instance, String)}
   *
   * Uses an internal cache and private constructor for storing a WeakReference of every Table.ID. Therefore, a Table.ID can't be instantiated outside this
   * class and is accessed by calling Table.ID.{@link #of(String)}.
   */
  public static class ID extends AbstractId {
    private static final long serialVersionUID = 7399913185860577809L;
    static final Cache<String,ID> cache = CacheBuilder.newBuilder().weakValues().build();

    public static final ID METADATA = of("!0");
    public static final ID REPLICATION = of("+rep");
    public static final ID ROOT = of("+r");

    private ID(final String canonical) {
      super(canonical);
    }

    /**
     * Get a Table.ID object for the provided canonical string.
     *
     * @param canonical
     *          table ID string
     * @return Table.ID object
     */
    public static ID of(final String canonical) {
      try {
        return cache.get(canonical, () -> new Table.ID(canonical));
      } catch (ExecutionException e) {
        throw new AssertionError("This should never happen: ID constructor should never return null.");
      }
    }
  }

}
