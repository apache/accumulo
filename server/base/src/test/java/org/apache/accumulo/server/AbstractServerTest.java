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
package org.apache.accumulo.server;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class AbstractServerTest {

  @Test
  public void testGracefulShutdown() throws Exception {
    ServerContext context = MockServerContext.get();
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());

    AuditedSecurityOperation secOp = createMock(AuditedSecurityOperation.class);
    expect(context.getSecurityOperation()).andReturn(secOp).times(2);

    // create some fake users
    TCredentials authorizedUser =
        new Credentials("authorizedUser", new PasswordToken("pass")).toThrift(instanceId);
    TCredentials unauthorizedUser =
        new Credentials("unauthorizedUser", new PasswordToken("pass")).toThrift(instanceId);
    expect(secOp.canPerformSystemActions(authorizedUser)).andReturn(true).once();
    expect(secOp.canPerformSystemActions(unauthorizedUser)).andReturn(false).once();

    // make a partial mock to use the mock ServerContext, and initialize the fields that will be
    // accessed in the call to gracefulShutdown below
    AbstractServer server =
        createMockBuilder(AbstractServer.class).addMockedMethod("getContext").createMock();
    expect(server.getContext()).andReturn(context).atLeastOnce();
    Field logField = AbstractServer.class.getDeclaredField("log");
    logField.setAccessible(true);
    logField.set(server, LoggerFactory.getLogger(AbstractServer.class));
    Field shutdownField = AbstractServer.class.getDeclaredField("shutdownRequested");
    shutdownField.setAccessible(true);
    shutdownField.set(server, new AtomicBoolean(false));

    replay(context, secOp, server);

    // verify that the unauthorized user can't change the state, but the authorized user can
    assertFalse(server.isShutdownRequested());
    server.gracefulShutdown(unauthorizedUser);
    assertFalse(server.isShutdownRequested());
    server.gracefulShutdown(authorizedUser);
    assertTrue(server.isShutdownRequested());

    verify(context, secOp, server);
  }

}
