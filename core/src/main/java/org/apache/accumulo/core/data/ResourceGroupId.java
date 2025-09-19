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

import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;

import com.github.benmanes.caffeine.cache.Cache;

public class ResourceGroupId extends AbstractId<ResourceGroupId> {

  private static final Pattern GROUP_NAME_PATTERN = Pattern.compile("^[a-zA-Z]+(_?[a-zA-Z0-9])*$");

  // cache is for canonicalization/deduplication of created objects,
  // to limit the number of ResourceGroupId objects in the JVM at any given moment
  // WeakReferences are used because we don't need them to stick around any longer than they need to
  static final Cache<String,ResourceGroupId> cache = Caches.getInstance()
      .createNewBuilder(CacheName.RESOURCE_GROUP_ID, false).weakValues().build();

  public static final ResourceGroupId DEFAULT =
      ResourceGroupId.of(Constants.DEFAULT_RESOURCE_GROUP_NAME);

  private static final long serialVersionUID = 1L;

  private ResourceGroupId(String canonical) {
    super(canonical);
    validateGroupName(canonical);
  }

  /**
   * @throws IllegalArgumentException if the group name is invalid
   */
  private static void validateGroupName(String groupName) {
    if (!GROUP_NAME_PATTERN.matcher(groupName).matches()) {
      throw new IllegalArgumentException("Group name '" + groupName
          + "' is invalid. Valid names must match the pattern: " + GROUP_NAME_PATTERN.pattern());
    }
  }

  /**
   * Get a ResourceGroupId object for the provided canonical string.
   *
   * @param canonical resource group ID string
   * @return ResourceGroupId object
   */
  public static ResourceGroupId of(final String canonical) {
    return cache.get(canonical, k -> new ResourceGroupId(canonical));
  }

}
