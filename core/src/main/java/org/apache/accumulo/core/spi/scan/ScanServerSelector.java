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

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

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
  default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  /**
   * This interface exists so that is easier to evolve what is passed to
   * {@link ScanServerSelector#init(InitParameters)} without having to make breaking changes.
   *
   * @since 2.1.0
   */
  interface InitParameters {

    /**
     * @return Options that were set in the client config using the prefix
     *         {@code scan.server.selector.opts.}. The prefix will be stripped. For example if
     *         {@code scan.server.selector.opts.k1=v1} is set in client config, then the returned
     *         map will contain {@code k1=v1}.
     */
    Map<String,String> getOptions();

    ServiceEnvironment getServiceEnv();

    /**
     * @return the set of live ScanServers. Each time the supplier is called it may return something
     *         different. A good practice would be to call this no more than once per a call to
     *         {@link ScanServerSelector#selectServers(SelectorParameters)} so that decisions are
     *         made using a consistent set of scan servers.
     */
    Supplier<Collection<ScanServerInfo>> getScanServers();

  }

  /**
   * This interface exists so that is easier to evolve what is passed to
   * {@link ScanServerSelector#selectServers(SelectorParameters)} without having to make breaking
   * changes.
   *
   * @since 2.1.0
   */
  interface SelectorParameters {

    /**
     * @return the set of tablets to be scanned
     */
    Collection<TabletId> getTablets();

    /**
     * @return scan attempt information for the tablet
     */
    Collection<? extends ScanServerAttempt> getAttempts(TabletId tabletId);

    /**
     * @return any hints set on a scanner using
     *         {@link org.apache.accumulo.core.client.ScannerBase#setExecutionHints(Map)}. If none
     *         were set, an empty map is returned.
     */
    Map<String,String> getHints();
  }

  /**
   * Uses the {@link SelectorParameters} to determine which, if any, ScanServer should be used for
   * scanning a tablet.
   *
   * @param params parameters for the calculation
   * @return results
   */
  ScanServerSelections selectServers(SelectorParameters params);

}
