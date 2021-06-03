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
package org.apache.accumulo.test;

import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.ThriftClientHandler;
import org.apache.thrift.TException;

public class NonCommittingExternalCompactionThriftClientHandler extends ThriftClientHandler
    implements TabletClientService.Iface {

  public NonCommittingExternalCompactionThriftClientHandler(TabletServer server) {
    super(server);
  }

  @Override
  public void compactionJobFinished(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent extent, long fileSize, long entries)
      throws ThriftSecurityException, TException {
    // do nothing
  }

  @Override
  public void compactionJobFailed(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent extent) throws TException {
    // do nothing
  }

}
