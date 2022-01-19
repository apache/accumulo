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

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.zookeeper.KeeperException;

public interface ScanServerLocator {

  class NoAvailableScanServerException extends Exception {

    private static final long serialVersionUID = 1L;

  }

  /**
   * Provide the ScanServerLocator implementation with a reference to the ClientContext in the case
   * that it needs it.
   *
   * @param ctx
   *          client context
   */
  void setClientContext(ClientContext ctx);

  /**
   * Called by the client to reserve an available Scan Server for running a scan on extent.
   *
   * @param extent
   *          extent to be scanned
   * @return address of scan server, in "host:port" format
   * @throws NoAvailableScanServerException
   *           when no scan server can be found
   */
  String reserveScanServer(KeyExtent extent)
      throws NoAvailableScanServerException, KeeperException, InterruptedException;

  /**
   * Called by the client to unreserve a Scan Server
   *
   * @param hostPort
   *          host and port of reserved scan server
   */
  void unreserveScanServer(String hostPort)
      throws NoAvailableScanServerException, KeeperException, InterruptedException;

}
