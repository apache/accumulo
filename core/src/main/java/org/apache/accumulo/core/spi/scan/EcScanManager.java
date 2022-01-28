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
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.base.Preconditions;

/**
 * @since 2.1.0
 */
// this plugin decides how to handle eventually consistent scans on the client side... long name
// could be EventuallyConsistentScan(Manager/Dispatcher/Govenor).. shorter name
// EcScan(Manager/Dispatcher/Govenor)
public interface EcScanManager {

  class ScanServerLocatorException extends Exception {
    private static final long serialVersionUID = 1L;

    public ScanServerLocatorException() {
      super();
    }

    public ScanServerLocatorException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  class NoAvailableScanServerException extends Exception {
    private static final long serialVersionUID = 1L;
  }

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
  public static class ScanAttempt {

    private final Action requestedAction;
    private final String server;
    private final long time;
    private final Result result;

    // represents reasons that previous attempts to scan failed
    enum Result {
      BUSY, IO_ERROR, ERROR, SUCCESS
    }

    public ScanAttempt(Action action, String server, long time, Result result) {
      this.requestedAction = action;
      this.server = server;
      this.time = time;
      this.result = result;
    }

    public long getTime() {
      return time;
    }

    public Result getResult() {
      return result;
    }
  }

  public interface ScanAttempts {
    List<ScanAttempt> all();

    List<ScanAttempt> forServer(String server);

    List<ScanAttempt> forTablet(TabletId tablet);
  }

  public interface DaParamaters {
    List<TabletId> getTablets();

    List<String> getScanServers();

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
  public interface EcScanActions {

    Action getAction(TabletId tablet);

    String getScanServer(TabletId tablet);

    Duration getDelay(String server);
  }

  EcScanActions determineActions(DaParamaters params);
}
