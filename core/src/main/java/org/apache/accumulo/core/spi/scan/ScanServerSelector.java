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

import org.apache.accumulo.core.client.ScannerBase;

import com.google.common.base.Preconditions;

/**
 * A client side plugin that determines what scan servers to use for eventually consistent scans.
 * When a scanner sets
 * {@link org.apache.accumulo.core.client.ScannerBase#setConsistencyLevel(ScannerBase.ConsistencyLevel)}
 * to {@link org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel#EVENTUAL} then this plugin
 * is used to determine which scan servers to use for a given tablet. To configure a class to use
 * for this plugin, set its name using the client config {@code scan.server.selector.impl}
 *
 * @since 2.1.0
 */
public interface ScanServerSelector {

  /**
   * The scan server group name that will be used when one is not specified.
   */
  String DEFAULT_SCAN_SERVER_GROUP_NAME = "default";

  /**
   * This method is called once after a {@link ScanServerSelector} is instantiated.
   */
  default void init(ScanServerSelectorInitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  /**
   * Uses the {@link ScanServerSelectorParameters} to determine which, if any, ScanServer should be used for scanning a tablet.
   *
   * @param params
   *          parameters for the calculation
   * @return results
   */
  ScanServerSelectorActions determineActions(ScanServerSelectorParameters params);

}
