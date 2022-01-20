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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.TabletId;

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

  /**
   * Provide the ScanServerLocator implementation with a reference to the ClientContext in the case
   * that it needs it.
   *
   * @param client
   *          accumulo client object
   */
  void setClient(AccumuloClient client);

  /**
   * Called by the client to reserve an available Scan Server for running a scan on extent.
   *
   * @param extent
   *          extent to be scanned
   * @return address of scan server, in "host:port" format
   * @throws NoAvailableScanServerException
   *           when no scan server can be found
   * @throws ScanServerLocatorException
   *           an error has occurred
   * @throws InterruptedException
   *           if any thread has interrupted the current thread.
   */
  String reserveScanServer(TabletId extent)
      throws NoAvailableScanServerException, ScanServerLocatorException, InterruptedException;

  /**
   * Called by the client to unreserve a Scan Server
   *
   * @param hostPort
   *          host and port of reserved scan server
   * @throws ScanServerLocatorException
   *           an error has occurred
   * @throws InterruptedException
   *           if any thread has interrupted the current thread.
   */
  void unreserveScanServer(String hostPort) throws ScanServerLocatorException, InterruptedException;

}
