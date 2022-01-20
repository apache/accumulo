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
package org.apache.accumulo.core.clientImpl;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ScanServerLocator;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;

public class DefaultScanServerLocator implements ScanServerLocator {

  private ClientContext context = null;
  private ZooReaderWriter zrw = null;

  @Override
  public void setClient(AccumuloClient ctx) {
    this.context = (ClientContext) ctx;
    this.zrw = new ZooReaderWriter(this.context.getConfiguration());
  }

  @Override
  public String reserveScanServer(TabletId extent)
      throws NoAvailableScanServerException, ScanServerLocatorException, InterruptedException {
    String server;
    try {
      server = ScanServerDiscovery.reserve(this.context.getZooKeeperRoot(), this.zrw);
    } catch (KeeperException | InterruptedException e) {
      throw new ScanServerLocatorException("Error reserving scan server", e);
    }
    if (server == null) {
      throw new NoAvailableScanServerException();
    }
    return server;
  }

  @Override
  public void unreserveScanServer(String hostPort)
      throws ScanServerLocatorException, InterruptedException {
    // The scan server calls ScanServerDiscovery.unreserve() on closeScan and closeMultiScan,
    // so this implementation does not do anything for this method.
  }

}
