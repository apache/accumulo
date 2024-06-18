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

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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

    /**
     * This function helps a scan server selector wait for an optional to become non-empty (like
     * waiting for scan servers to be present) and throws exceptions when waiting is no longer
     * possible OR returning false if the max wait time was exceeded. The passed in condition will
     * be periodically called and as long as it returns an empty optional the function will continue
     * to wait.
     *
     * @param condition periodically calls this to see if it is non-empty.
     * @param maxWaitTime the maximum time to wait for the condition to become non-empty
     * @param description a description of what is being waited on, used for error messages and
     *        logging
     * @return The first non-empty optional returned by the condition. An empty optional if the
     *         maxWaitTime was exceeded without the condition ever returning a non-empty optional.
     *
     * @throws org.apache.accumulo.core.client.TableDeletedException if the table is deleted while
     *         waiting for the condition to become non-empty. Do not catch this exception, let it
     *         escape.
     * @throws org.apache.accumulo.core.client.TimedOutException if the timeout specified by
     *         {@link ScannerBase#setTimeout(long, TimeUnit)} is exceeded while waiting. Do not
     *         catch this exception, let it escape.
     *
     * @since 4.0.0
     */
    public <T> Optional<T> waitUntil(Supplier<Optional<T>> condition, Duration maxWaitTime,
        String description);
  }

  /**
   * <p>
   * Uses the {@link SelectorParameters} to determine which, if any, ScanServer should be used for
   * scanning a tablet.
   * </p>
   *
   * <p>
   * In the case where there are zero scan servers available and an implementation does not want to
   * fall back to tablet servers, its ok to wait and poll for scan servers. When waiting its best to
   * use {@link SelectorParameters#waitUntil(Supplier, Duration, String)} as this allows Accumulo to
   * know about the wait and cancel it via exceptions when it no longer makes sense to wait.
   * </p>
   *
   * @param params parameters for the calculation
   * @return results
   */
  ScanServerSelections selectServers(SelectorParameters params);

}
