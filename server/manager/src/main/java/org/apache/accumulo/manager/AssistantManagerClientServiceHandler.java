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
package org.apache.accumulo.manager;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.*;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.manager.thrift.AssistantManagerClientService;
import org.apache.accumulo.core.manager.thrift.ThriftPropertyException;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.thrift.TException;
import org.slf4j.Logger;

public class AssistantManagerClientServiceHandler implements AssistantManagerClientService.Iface {

  private static final Logger log = Manager.log;
  private final Manager manager;
  private final ServerContext context;
  private final AuditedSecurityOperation security;

  protected AssistantManagerClientServiceHandler(Manager manager) {
    this.manager = manager;
    this.context = manager.getContext();
    this.security = context.getSecurityOperation();
  }

  private NamespaceId getNamespaceIdFromTableId(TableOperation tableOp, TableId tableId)
      throws ThriftTableOperationException {
    NamespaceId namespaceId;
    try {
      namespaceId = context.getNamespaceId(tableId);
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(tableId.canonical(), null, tableOp,
          TableOperationExceptionType.NOTFOUND, e.getMessage());
    }
    return namespaceId;
  }

  @Override
  public long initiateFlush(TInfo tinfo, TCredentials credentials, String tableName)
      throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, TException {
    return 0;
  }

  @Override
  public void waitForFlush(TInfo tinfo, TCredentials credentials, String tableName,
      ByteBuffer startRow, ByteBuffer endRow, long flushID, long maxLoops)
      throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, TException {

  }

  @Override
  public void setTableProperty(TInfo tinfo, TCredentials credentials, String tableName,
      String property, String value) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, ThriftPropertyException, TException {

  }

  @Override
  public void modifyTableProperties(TInfo tinfo, TCredentials credentials, String tableName,
      TVersionedProperties vProperties) throws ThriftSecurityException,
      ThriftTableOperationException, ThriftNotActiveServiceException,
      ThriftConcurrentModificationException, ThriftPropertyException, TException {

  }

  @Override
  public void removeTableProperty(TInfo tinfo, TCredentials credentials, String tableName,
      String property) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, TException {

  }

  @Override
  public void setNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property, String value) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, ThriftPropertyException, TException {

  }

  @Override
  public void modifyNamespaceProperties(TInfo tinfo, TCredentials credentials, String ns,
      TVersionedProperties vProperties) throws ThriftSecurityException,
      ThriftTableOperationException, ThriftNotActiveServiceException,
      ThriftConcurrentModificationException, ThriftPropertyException, TException {

  }

  @Override
  public void removeNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, TException {

  }

  @Override
  public void setSystemProperty(TInfo tinfo, TCredentials credentials, String property,
      String value) throws ThriftSecurityException, ThriftNotActiveServiceException,
      ThriftPropertyException, TException {

  }

  @Override
  public void modifySystemProperties(TInfo tinfo, TCredentials credentials,
      TVersionedProperties vProperties)
      throws ThriftSecurityException, ThriftNotActiveServiceException,
      ThriftConcurrentModificationException, ThriftPropertyException, TException {

  }

  @Override
  public void removeSystemProperty(TInfo tinfo, TCredentials credentials, String property)
      throws ThriftSecurityException, ThriftNotActiveServiceException, TException {

  }
}
