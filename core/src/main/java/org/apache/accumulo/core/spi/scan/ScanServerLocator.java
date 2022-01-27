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

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import java.util.List;
import java.util.Map;

/**
 * @since 2.1.0
 */
// TODO maybe name ScanServerAssigner ... it assigns a tablet to scan server
public interface ScanServerLocator {

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

  public static class ScanAttempt {

    private final String server;
    private final long time;
    private final Result result;

    // represents reasons that previous attempts to scan failed
    enum Result {
      BUSY,
      IO_ERROR,
      NO_SCAN_SERVERS,
      NO_TABLET_SERVERS
    }

    public ScanAttempt(String server, long time, Result result) {
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

  public interface LocateParameters {
    List<TabletId> getTablets();

    List<String> getScanServers();

    ScanAttempts getScanAttempts();

  }

  public interface LocateResult {
    String getServer(TabletId tablet);

    long getSleepTime(String server);
  }

  LocateResult locate(LocateParameters params);
}
