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
package org.apache.accumulo.test;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.tserver.ScanServer;
import org.apache.accumulo.tserver.TabletHostingServer;
import org.apache.thrift.TException;

/**
 * ScanServer implementation that will stop itself after the the 3rd scan batch scan
 *
 */
public class SelfStoppingScanServer extends ScanServer
    implements TabletScanClientService.Iface, TabletHostingServer {

  private final AtomicInteger scanCount = new AtomicInteger(0);

  public SelfStoppingScanServer(ScanServerOpts opts, String[] args) {
    super(opts, args);
  }

  @Override
  public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException, TException {
    scanCount.incrementAndGet();
    super.closeMultiScan(tinfo, scanID);
    if (scanCount.get() == 3) {
      serverStopRequested = true;
    }
  }

  public static void main(String[] args) throws Exception {
    try (SelfStoppingScanServer tserver = new SelfStoppingScanServer(new ScanServerOpts(), args)) {
      tserver.runServer();
    }
  }

}
