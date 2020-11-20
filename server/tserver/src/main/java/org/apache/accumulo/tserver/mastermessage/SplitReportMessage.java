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
package org.apache.accumulo.tserver.mastermessage;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.TabletSplit;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

public class SplitReportMessage implements MasterMessage {
  private Map<KeyExtent,Text> extents;
  private KeyExtent old_extent;

  public SplitReportMessage(KeyExtent old_extent, KeyExtent ne1, Text np1, KeyExtent ne2,
      Text np2) {
    this.old_extent = old_extent;
    extents = new TreeMap<>();
    extents.put(ne1, np1);
    extents.put(ne2, np2);
  }

  @Override
  public void send(TCredentials credentials, String serverName, MasterClientService.Iface client)
      throws TException, ThriftSecurityException {
    TabletSplit split = new TabletSplit();
    split.oldTablet = old_extent.toThrift();
    split.newTablets =
        extents.keySet().stream().map(KeyExtent::toThrift).collect(Collectors.toList());
    client.reportSplitExtent(TraceUtil.traceInfo(), credentials, serverName, split);
  }

}
