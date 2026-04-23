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
package org.apache.accumulo.test.rpc;

import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.test.rpc.thrift.SimpleThriftService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for starting a simple thrift server, and providing a corresponding client for in-memory
 * testing of thrift behavior
 */
public class SimpleThriftServiceRunner {

  private SimpleThriftServiceHandler handler = new SimpleThriftServiceHandler();

  private final Mocket mocket;
  private final Thread serviceThread;
  private final TServer server;

  public SimpleThriftServiceRunner(String threadName) {
    this.mocket = new Mocket();
    this.server = createServer();
    this.serviceThread = new Thread(server::serve, threadName);
  }

  public void startService() {
    serviceThread.start();
  }

  public SimpleThriftServiceHandler handler() {
    return handler;
  }

  public SimpleThriftService.Client client() {
    return new SimpleThriftService.Client(
        new LoggingBinaryProtocol("client", mocket.getClientTransport()));
  }

  private static class LoggingBinaryProtocol extends TBinaryProtocol {
    private static final Logger log = LoggerFactory.getLogger(LoggingBinaryProtocol.class);
    private final String type;

    private LoggingBinaryProtocol(String type, TTransport trans) {
      super(trans);
      this.type = type;
    }

    @Override
    public TMessage readMessageBegin() throws TException {
      TMessage message = super.readMessageBegin();
      log.debug("{}:readMessageBegin:{}", type, message);
      return message;
    }

    @Override
    public void readMessageEnd() throws TException {
      log.debug("{}:readMessageEnd", type);
      super.readMessageEnd();
    }

    @Override
    public void writeMessageBegin(TMessage message) throws TException {
      log.debug("{}:writeMessageBegin:{}", type, message);
      super.writeMessageBegin(message);
    }

    @Override
    public void writeMessageEnd() throws TException {
      log.debug("{}:writeMessageEnd", type);
      super.writeMessageEnd();
    }
  }

  private static class LoggingProtocolFactory implements TProtocolFactory {
    private static final long serialVersionUID = 1L;
    private final String type;

    LoggingProtocolFactory(String clientOrServer) {
      this.type = clientOrServer;
    }

    @Override
    public TProtocol getProtocol(TTransport trans) {
      return new LoggingBinaryProtocol(type, trans);
    }
  }

  private TServer createServer() {
    TServer.Args args = new TServer.Args(mocket.getServerTransport());
    SimpleThriftService.Iface actualHandler = TraceUtil.wrapService(handler);
    args.processor(new SimpleThriftService.Processor<>(actualHandler));
    args.inputProtocolFactory(new LoggingProtocolFactory("serverInput"));
    args.outputProtocolFactory(new LoggingProtocolFactory("serverOutput"));
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
