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

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.thrift.metrics.ThriftMetrics;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class TServerUtils {
  private static final Logger log = Logger.getLogger(TServerUtils.class);
  
  public static ThreadLocal<String> clientAddress = new ThreadLocal<String>();
  
  // Use NIO sockets to create a TServer so our shutdown function will work
  public static TServerTransport openPort(int port) throws IOException {
    ServerSocket sock = ServerSocketChannel.open().socket();
    sock.setReuseAddress(true);
    sock.bind(new InetSocketAddress(port));
    return new TServerSocket(sock);
  }
  
  public static class ServerPort {
    public final TServer server;
    public final int port;
    
    public ServerPort(TServer server, int port) {
      this.server = server;
      this.port = port;
    }
  }
  
  /**
   * Start a server, at the given port, or higher, if that port is not available.
   * 
   * @param portHint
   *          the port to attempt to open, can be zero, meaning "any available port"
   * @param processor
   *          the service to be started
   * @param serverName
   *          the name of the class that is providing the service
   * @param threadName
   *          name this service's thread for better debugging
   * @return the server object created, and the port actually used
   * @throws UnknownHostException
   *           when we don't know our own address
   */
  public static ServerPort startServer(Property portHintProperty, TProcessor processor, String serverName, String threadName, boolean portSearch)
      throws UnknownHostException {
    int portHint = AccumuloConfiguration.getSystemConfiguration().getPort(portHintProperty);
    for (int j = 0; j < 100; j++) {
      
      // Are we going to slide around, looking for an open port?
      int portsToSearch = 1;
      if (portSearch)
        portsToSearch = 1000;
      
      for (int i = 0; i < portsToSearch; i++) {
        int port = portHint + i;
        if (port > 65535)
          port = 1024 + port % (65535 - 1024);
        TServerTransport serverTransport;
        try {
          serverTransport = TServerUtils.openPort(port);
          TServer server = TServerUtils.startTServer(processor, serverTransport, serverName, threadName, -1);
          return new ServerPort(server, port);
        } catch (IOException ex) {
          if (portHint == 0) {
            throw new RuntimeException(ex);
          }
          log.info("Unable to use port " + port + ", retrying. (Thread Name = " + threadName + ")");
          UtilWaitThread.sleep(250);
        }
      }
    }
    throw new UnknownHostException("Unable to find a listen port");
  }
  
  public static class TimedProcessor implements TProcessor {
    
    final TProcessor other;
    ThriftMetrics metrics = null;
    long idleStart = 0;
    
    TimedProcessor(TProcessor next, String serverName, String threadName) {
      this.other = next;
      // Register the metrics MBean
      try {
        metrics = new ThriftMetrics(serverName, threadName);
        metrics.register();
      } catch (Exception e) {
        log.error("Exception registering MBean with MBean Server", e);
      }
      idleStart = System.currentTimeMillis();
    }
    
    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
      long now = 0;
      if (metrics.isEnabled()) {
        now = System.currentTimeMillis();
        metrics.add(ThriftMetrics.idle, (now - idleStart));
      }
      try {
        return other.process(in, out);
      } finally {
        if (metrics.isEnabled()) {
          idleStart = System.currentTimeMillis();
          metrics.add(ThriftMetrics.execute, idleStart - now);
        }
      }
    }
  }
  
  public static class ClientInfoProcessorFactory extends TProcessorFactory {
    
    public ClientInfoProcessorFactory(TProcessor processor) {
      super(processor);
    }
    
    public TProcessor getProcessor(TTransport trans) {
      if (trans instanceof TSocket) {
        TSocket tsock = (TSocket) trans;
        clientAddress.set(tsock.getSocket().getInetAddress().getHostAddress() + ":" + tsock.getSocket().getPort());
      } else {
        clientAddress.set(null);
      }
      
      return super.getProcessor(trans);
    }
  }
  
  // Boilerplate start-up for a TServer
  public static TServer startTServer(TProcessor processor, TServerTransport serverTransport, String serverName, String threadName, int numThreads) {
    TThreadPoolServer.Options options = new TThreadPoolServer.Options();
    if (numThreads > 0)
      options.maxWorkerThreads = numThreads;
    processor = new TServerUtils.TimedProcessor(processor, serverName, threadName);
    final TServer tserver = new TThreadPoolServer(new ClientInfoProcessorFactory(processor), serverTransport, ThriftUtil.transportFactory(),
        ThriftUtil.transportFactory(), ThriftUtil.inputProtocolFactory(), ThriftUtil.outputProtocolFactory(), options);
    Runnable serveTask = new Runnable() {
      public void run() {
        try {
          tserver.serve();
        } catch (Error e) {
          Halt.halt("Unexpected error in TThreadPoolServer " + e + ", halting.");
        }
      }
    };
    serveTask = new LoggingRunnable(TServerUtils.log, serveTask);
    Thread thread = new Daemon(serveTask, threadName);
    thread.start();
    return tserver;
  }
  
  // Existing connections will keep our thread running: reach in with reflection and insist that they shutdown.
  public static void stopTServer(TServer s) {
    if (s == null)
      return;
    s.stop();
    try {
      Field f = s.getClass().getDeclaredField("executorService_");
      f.setAccessible(true);
      ExecutorService es = (ExecutorService) f.get(s);
      es.shutdownNow();
    } catch (Exception e) {
      TServerUtils.log.error("Unable to call shutdownNow", e);
    }
  }
}
