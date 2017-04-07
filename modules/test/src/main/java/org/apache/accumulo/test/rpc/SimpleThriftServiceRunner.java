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
package org.apache.accumulo.test.rpc;

import org.apache.accumulo.server.rpc.RpcWrapper;
import org.apache.accumulo.test.rpc.thrift.SimpleThriftService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;

/**
 * A utility for starting a simple thrift server, and providing a corresponding client for in-memory testing of thrift behavior
 */
public class SimpleThriftServiceRunner {

  private SimpleThriftServiceHandler handler = new SimpleThriftServiceHandler();

  private final Mocket mocket;
  private final Thread serviceThread;
  private final TServer server;

  public SimpleThriftServiceRunner(String threadName, boolean useWrapper) {
    this.mocket = new Mocket();
    this.server = createServer(useWrapper);
    this.serviceThread = new Thread(() -> server.serve(), threadName);
  }

  public void startService() {
    serviceThread.start();
  }

  public SimpleThriftServiceHandler handler() {
    return handler;
  }

  public SimpleThriftService.Client client() {
    return new SimpleThriftService.Client(new TBinaryProtocol(mocket.getClientTransport()));
  }

  private TServer createServer(boolean useWrapper) {
    TServer.Args args = new TServer.Args(mocket.getServerTransport());
    SimpleThriftService.Iface actualHandler = handler;
    if (useWrapper) {
      actualHandler = RpcWrapper.<SimpleThriftService.Iface> service(handler);
    }
    args.processor(new SimpleThriftService.Processor<>(actualHandler));
    args.protocolFactory(new TBinaryProtocol.Factory());
    return new TSimpleServer(args);
  }

  public void stopService() {
    server.stop();
    try {
      serviceThread.join();
    } catch (InterruptedException e) {
      // re-set interrupt flag
      Thread.currentThread().interrupt();
    }
  }

}
