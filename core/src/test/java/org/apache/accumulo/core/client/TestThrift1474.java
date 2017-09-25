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
package org.apache.accumulo.core.client;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTest;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Test;

public class TestThrift1474 {

  static class TestServer implements ThriftTest.Iface {

    @Override
    public boolean success() throws TException {
      return true;
    }

    @Override
    public boolean fails() throws TException {
      return false;
    }

    @Override
    public boolean throwsError() throws ThriftSecurityException, TException {
      throw new ThriftSecurityException();
    }

  }

  @Test
  public void test() throws IOException, TException, InterruptedException {
    TServerSocket serverTransport = new TServerSocket(0);
    serverTransport.listen();
    int port = serverTransport.getServerSocket().getLocalPort();
    TestServer handler = new TestServer();
    ThriftTest.Processor<ThriftTest.Iface> processor = new ThriftTest.Processor<>(handler);

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
    args.stopTimeoutVal = 10;
    args.stopTimeoutUnit = TimeUnit.MILLISECONDS;
    final TServer server = new TThreadPoolServer(args.processor(processor));
    Thread thread = new Thread() {
      @Override
      public void run() {
        server.serve();
      }
    };
    thread.start();
    while (!server.isServing()) {
      sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    }

    TTransport transport = new TSocket("localhost", port);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    ThriftTest.Client client = new ThriftTest.Client(protocol);
    assertTrue(client.success());
    assertFalse(client.fails());
    try {
      client.throwsError();
      fail("no exception thrown");
    } catch (ThriftSecurityException ex) {
      // expected
    }
    server.stop();
    thread.join();
  }

}
