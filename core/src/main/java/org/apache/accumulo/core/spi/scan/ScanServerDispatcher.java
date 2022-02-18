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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

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
  }

  /**
   * This method is called once after a ScanDispatcher is instantiated.
   */
  default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  // this object is used to communicate what the previous actions were attempted, when they were
  // attempted, and the result of the attempt
  interface ScanAttempt extends Comparable<ScanAttempt> {

    // represents reasons that previous attempts to scan failed
    enum Result {
      BUSY, IO_ERROR, ERROR, SUCCESS
    }

    long getTime();

    Result getResult();

    ScanServerDispatcher.Action getAction();

    String getServer();

    TabletId getTablet();
  }

  public interface ScanAttempts {
    Collection<ScanAttempt> all();

    SortedSet<ScanAttempt> forServer(String server);

    SortedSet<ScanAttempt> forTablet(TabletId tablet);
  }

  public interface DispatcherParameters {

    /**
     * @return the set of tablets to be scanned
     */
    Collection<TabletId> getTablets();

    /**
     * @return the set of live ScanServers
     */
    Set<String> getScanServers();

    /**
     * @return a list of ScanServers in order (TODO: what type of order?)
     */
    List<String> getOrderedScanServers();

    /**
     * @return scan attempt information (TODO: how is this used?)
     */
    ScanAttempts getScanAttempts();
  }

  public enum Action {
    WAIT, USE_SCAN_SERVER, USE_TABLET_SERVER,
    // TODO remove... leaving here now to help think through things
    // USE_FILES ... thinking about this possibility made me change names from scan server specific
    // to more general eventual consistent handling
  }

  // TODO need a better name.. this interface is used to communicate what actions the plugin would
  // like Accumulo to take for the scan... maybe EcScanActions
  public interface ScanServerDispatcherResults {

    Action getAction(TabletId tablet);

    String getScanServer(TabletId tablet);

    Duration getDelay(String server);
  }

  /**
   * Uses the DispatcherParameters to determine which, if any, ScanServer should be used for
   * scanning a tablet.
   *
   * @param params
   *          parameters for the calculation
   * @return results
   */
  ScanServerDispatcherResults determineActions(DispatcherParameters params);
}
