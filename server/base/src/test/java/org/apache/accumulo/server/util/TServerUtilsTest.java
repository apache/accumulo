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
package org.apache.accumulo.server.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;

import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerSocket;
import org.junit.Test;

public class TServerUtilsTest {
  private static class TServerWithoutES extends TServer {
    boolean stopCalled;

    TServerWithoutES(TServerSocket socket) {
      super(new TServer.Args(socket));
      stopCalled = false;
    }

    @Override
    public void serve() {}

    @Override
    public void stop() {
      stopCalled = true;
    }
  }

  private static class TServerWithES extends TServerWithoutES {
    final ExecutorService executorService_;

    TServerWithES(TServerSocket socket) {
      super(socket);
      executorService_ = createMock(ExecutorService.class);
      expect(executorService_.shutdownNow()).andReturn(null);
      replay(executorService_);
    }
  }

  @Test
  public void testStopTServer_ES() {
    TServerSocket socket = createNiceMock(TServerSocket.class);
    TServerWithES s = new TServerWithES(socket);
    TServerUtils.stopTServer(s);
    assertTrue(s.stopCalled);
    verify(s.executorService_);
  }

  @Test
  public void testStopTServer_NoES() {
    TServerSocket socket = createNiceMock(TServerSocket.class);
    TServerWithoutES s = new TServerWithoutES(socket);
    TServerUtils.stopTServer(s);
    assertTrue(s.stopCalled);
  }

  @Test
  public void testStopTServer_Null() {
    TServerUtils.stopTServer(null);
    // not dying is enough
  }
}
