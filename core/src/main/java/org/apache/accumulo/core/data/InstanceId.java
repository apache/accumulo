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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * A strongly typed representation of an Accumulo instance ID. The constructor for this class will
 * throw an error if the canonical parameter is null.
 *
 * @since 2.1.0
 */
public class InstanceId extends AbstractId<InstanceId> {
  private static final long serialVersionUID = 1L;
  // cache is for canonicalization/deduplication of created objects,
  // to limit the number of InstanceId objects in the JVM at any given moment
  // WeakReferences are used because we don't need them to stick around any longer than they need to
  static final Cache<String,InstanceId> cache = CacheBuilder.newBuilder().weakValues().build();

  private InstanceId(String canonical) {
    super(canonical);
  }

  /**
   * Get a InstanceId object for the provided canonical string. This is guaranteed to be non-null
   *
   * @param canonical Instance ID string
   * @return InstanceId object
   */
  public static InstanceId of(final String canonical) {
    try {
      return cache.get(canonical, () -> new InstanceId(canonical));
    } catch (ExecutionException e) {
      throw new AssertionError(
          "This should never happen: ID constructor should never return null.");
    }
  }

  /**
   * Get a InstanceId object for the provided uuid. This is guaranteed to be non-null
   *
   * @param uuid UUID object
   * @return InstanceId object
   */
  public static InstanceId of(final UUID uuid) {
    return of(Objects.requireNonNull(uuid, "uuid cannot be null").toString());
  }
}
