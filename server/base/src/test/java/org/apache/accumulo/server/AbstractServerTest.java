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

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class AbstractServerTest {
  @Test
  public void testError() throws Exception {
    AbstractServer server = mock(AbstractServer.class);
    server.runServer();
    expectLastCall().andThrow(new UnknownError());
    server.close();
    expectLastCall().andThrow(new IllegalStateException());

    replay(server);
    var error = assertThrows(UnknownError.class, () -> AbstractServer.startServer(server,
        LoggerFactory.getLogger(AbstractServerTest.class)));
    assertEquals(1, error.getSuppressed().length);
    assertEquals(IllegalStateException.class, error.getSuppressed()[0].getClass());
    verify(server);
  }
}
