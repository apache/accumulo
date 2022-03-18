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
package org.apache.accumulo.core.spi.scan;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.base.Preconditions;

/**
 * @since 2.1.0
 */
// this plugin decides how to handle eventually consistent scans on the client side... long name
// could be EventuallyConsistentScan(Manager/Dispatcher/Govenor).. shorter name
// EcScan(Manager/Dispatcher/Govenor)
public interface ScanServerDispatcher {

  public interface InitParameters {
    Map<String,String> getOptions();

    ServiceEnvironment getServiceEnv();

    /**
     * @return the set of live ScanServers.
     */
    Supplier<Set<String>> getScanServers();
  }

  /**
   * This method is called once after a ScanDispatcher is instantiated.
   */
  default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  // this object is used to communicate what the previous actions were attempted, when they were
  // attempted, and the result of the attempt
  interface ScanAttempt {

    // represents reasons that previous attempts to scan failed
    enum Result {
      BUSY, ERROR
    }

    String getServer();

    long getEndTime();

    Result getResult();
  }

  public interface DispatcherParameters {

    /**
     * @return the set of tablets to be scanned
     */
    Collection<TabletId> getTablets();

    /**
     * @return scan attempt information (TODO: how is this used?)
     */
    Collection<? extends ScanAttempt> getAttempts(TabletId tabletId);
  }

  public interface Actions {

    /**
     * @return what scan server to use for a given tablet. Returning null indicates the tablet
     *         server should be used for this tablet.
     */
    String getScanServer(TabletId tabletId);

    /**
     * @return The amount of time to wait on the client side before starting to contact servers.
     *         Return {@link Duration#ZERO} if no client side wait is desired.
     */
    public Duration getDelay();

    /**
     * @return The amount of time to wait for a scan to start on the server side before reporting
     *         busy. For example if a scan request is sent to scan server with a busy timeout of
     *         50ms and the scan has not started running within that time then the scan server will
     *         not ever run the scan and it will report back busy. If the scan starts running, then
     *         it will never report back busy. Setting a busy timeout that is &le; 0 means that it
     *         will wait indefinitely on the server side for the task to start.
     */
    public Duration getBusyTimeout();
  }

  /**
   * Uses the DispatcherParameters to determine which, if any, ScanServer should be used for
   * scanning a tablet.
   *
   * @param params
   *          parameters for the calculation
   * @return results
   */
  Actions determineActions(DispatcherParameters params);
}
