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
package org.apache.accumulo.core.spi.scan;

import java.util.Comparator;
import java.util.Map;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * A factory for creating comparators used for prioritizing scans. For information about
 * configuring, find the documentation for the {@code tserver.scan.executors.} property.
 *
 * @since 2.0.0
 * @see org.apache.accumulo.core.spi
 */
public interface ScanPrioritizer {

  /**
   * The method parameters for {@link ScanPrioritizer#createComparator(CreateParameters)}. This
   * interface exists so the API can evolve and additional parameters can be passed to the method in
   * the future.
   *
   * @since 2.0.0
   */
  public static interface CreateParameters {
    /**
     * @return The options configured for the scan prioritizer with properties of the form
     *         {@code tserver.scan.executors.<name>.prioritizer.opts.<key>=<value>}. Only the
     *         {@code <key>=<value>} portions of those properties ends up in the returned map.
     */
    Map<String,String> getOptions();

    ServiceEnvironment getServiceEnv();
  }

  Comparator<ScanInfo> createComparator(CreateParameters params);
}
