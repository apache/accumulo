/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.admin;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.impl.CloneConfigurationImpl;

/**
 * A configuration object that can be used with the table clone command in the
 * {@link TableOperations}.
 *
 * @since 1.10 and 2.1
 */
public interface CloneConfiguration {
  /**
   * Determines if memory is flushed in the source table before cloning.
   *
   * @return true if memory is flushed in the source table before cloning.
   */
  public boolean isFlush();

  /**
   * The source table properties are copied. This allows overriding of some of those properties.
   *
   * @return The source table properties to override.
   */
  public Map<String,String> getPropertiesToSet();

  /**
   * The source table properties are copied, this allows reverting to system defaults for some of
   * those properties.
   *
   * @return The properties that are to be reverted to system defaults.
   */
  public Set<String> getPropertiesToExclude();

  /**
   * The new table is normally brought online after the cloning process. This allows leaving the new
   * table offline
   *
   * @return true if the new table is to be kept offline after cloning.
   */
  public boolean isKeepOffline();

  /**
   * A CloneConfiguration builder
   *
   * @since 1.10 and 2.1
   */
  public static interface Builder {
    /**
     * Determines if memory is flushed in the source table before cloning.
     *
     * @param flush
     *          true if memory is flushed in the source table before cloning.
     */
    public Builder setFlush(boolean flush);

    /**
     * The source table properties are copied. This allows overriding of some of those properties.
     *
     * @param propertiesToSet
     *          The source table properties to override.
     */
    public Builder setPropertiesToSet(Map<String,String> propertiesToSet);

    /**
     * The source table properties are copied, this allows reverting to system defaults for some of
     * those properties.
     *
     * @param propertiesToExclude
     *          The properties that are to be reverted to system defaults.
     */
    public Builder setPropertiesToExclude(Set<String> propertiesToExclude);

    /**
     * The new table is normally brought online after the cloning process. This allows leaving the
     * new table offline
     *
     * @param keepOffline
     *          true if the new table is to be kept offline after cloning.
     */
    public Builder setKeepOffline(boolean keepOffline);

    /**
     * Build the clone configuration
     *
     * @return the built immutable clone configuration
     */
    public CloneConfiguration build();
  }

  /**
   * @return a {@link CloneConfiguration} builder
   */
  public static CloneConfiguration.Builder builder() {
    return new CloneConfigurationImpl();
  }

}
