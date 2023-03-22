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
package org.apache.accumulo.core.spi.ondemand;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TabletId;

/**
 * Object used by the TabletServer to determine which onDemand Tablets to unload for a Table
 *
 * @since 3.1.0
 */
public interface OnDemandTabletUnloader {

  interface UnloaderParams {

    /**
     * @return table configuration
     * @since 3.1.0
     */
    Map<String,String> getTableConfiguration();

    /**
     * Returns the onDemand tablets that are currently online and the time that they were last
     * accessed
     *
     * @since 3.1.0
     */
    Map<TabletId,Long> getOnDemandTablets();

    /**
     * Called by the implementation to inform the TabletServer as to which onDemand tablets should
     * be unloaded
     *
     * @param tablets onDemand Tablets to unload
     * @since 3.1.0
     */
    void setOnDemandTabletsToUnload(Set<TabletId> tablets);

  }

  /**
   * Implementations will evaluate each entry returned from
   * {@link UnloaderParams#getOnDemandTablets()} and call
   * {@link UnloaderParams#setOnDemandTabletsToUnload(Set)} with the onDemand tablets that should be
   * unloaded by the TabletServer
   *
   * @param params UnloaderParams object
   * @since 3.1.0
   */
  void evaluate(UnloaderParams params);
}
