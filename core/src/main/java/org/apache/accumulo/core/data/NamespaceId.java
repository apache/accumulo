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
package org.apache.accumulo.core.data;

import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * A strongly typed representation of a namespace ID. This class cannot be used to get a namespace
 * ID from a namespace name, but does provide the namespace ID string wrapped with a stronger type.
 * The constructor for this class will throw an error if the canonical parameter is null.
 *
 * @since 2.0.0
 */
public class NamespaceId extends AbstractId<NamespaceId> {
  private static final long serialVersionUID = 1L;
  // cache is for canonicalization/deduplication of created objects,
  // to limit the number of NamespaceId objects in the JVM at any given moment
  // WeakReferences are used because we don't need them to stick around any longer than they need to
  static final Cache<String,NamespaceId> cache = CacheBuilder.newBuilder().weakValues().build();

  private NamespaceId(String canonical) {
    super(canonical);
  }

  /**
   * Get a NamespaceId object for the provided canonical string. This is guaranteed to be non-null
   *
   * @param canonical Namespace ID string
   * @return NamespaceId object
   */
  public static NamespaceId of(final String canonical) {
    try {
      return cache.get(canonical, () -> new NamespaceId(canonical));
    } catch (ExecutionException e) {
      throw new AssertionError(
          "This should never happen: ID constructor should never return null.");
    }
  }
}
