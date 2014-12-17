/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.tserver.mastermessage;

import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.MasterClientService.Iface;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.thrift.TException;

public class TabletStatusMessage implements MasterMessage {
  
  private KeyExtent extent;
  private TabletLoadState status;
  
  public TabletStatusMessage(TabletLoadState status, KeyExtent extent) {
    this.extent = extent;
    this.status = status;
  }
  
  public void send(TCredentials auth, String serverName, Iface client) throws TException, ThriftSecurityException {
    client.reportTabletStatus(Tracer.traceInfo(), auth, serverName, status, extent.toThrift());
  }
}
