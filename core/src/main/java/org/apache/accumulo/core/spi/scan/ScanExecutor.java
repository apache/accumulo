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

import java.util.Map;
import java.util.Optional;

/**
 * Interface for obtaining information about a scan executor
 *
 * @since 2.0.0
 */
public interface ScanExecutor {

  interface Config {
    /**
     * @return the unique name used to identified executor in config
     */
    String getName();

    /**
     * @return the max number of threads that were configured
     */
    int getMaxThreads();

    /**
     * @return the prioritizer that was configured
     */
    Optional<String> getPrioritizerClass();

    /**
     * @return the prioritizer options
     */
    Map<String,String> getPrioritizerOptions();
  }

  /**
   * @return The number of task queued for the executor
   */
  int getQueued();

  /**
   * @return The configuration used to create the executor
   */
  Config getConfig();
}
