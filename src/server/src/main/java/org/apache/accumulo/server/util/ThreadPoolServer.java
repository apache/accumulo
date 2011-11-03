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

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/* A simple version of Thrift's TThreadPoolServer, 
 * with a bounded number of threads, and an unbounded work queue
 */

public class ThreadPoolServer extends TServer {
  
  // Executor service for handling client connections
  private ExecutorService executorService_;
  
  // Flag for stopping the server
  private volatile boolean stopped_;
  
  public ThreadPoolServer(TProcessor processor, TServerTransport serverTransport, int threads) {
    super(new TProcessorFactory(processor), serverTransport);
    executorService_ = Executors.newFixedThreadPool(threads);
  }
  
  public ThreadPoolServer(TProcessor processor, TServerTransport serverTransport) {
    super(new TProcessorFactory(processor), serverTransport);
    executorService_ = Executors.newFixedThreadPool(5);
  }
  
  public void serve() {
    try {
      serverTransport_.listen();
    } catch (TTransportException ttx) {
      ttx.printStackTrace();
      return;
    }
    
    stopped_ = false;
    while (!stopped_) {
      try {
        TTransport client = serverTransport_.accept();
        WorkerProcess wp = new WorkerProcess(client);
        executorService_.execute(wp);
      } catch (TTransportException ttx) {
        ttx.printStackTrace();
      }
    }
    
    executorService_.shutdown();
    try {
      
      executorService_.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException ix) {
      // Ignore and more on
    }
  }
  
  public void stop() {
    stopped_ = true;
    serverTransport_.interrupt();
  }
  
  private class WorkerProcess implements Runnable {
    
    /**
     * Client that this services.
     */
    private TTransport client_;
    
    /**
     * Default constructor.
     * 
     * @param client
     *          Transport to process
     */
    private WorkerProcess(TTransport client) {
      client_ = client;
    }
    
    /**
     * Loops on processing a client forever
     */
    public void run() {
      TProcessor processor = null;
      TTransport inputTransport = null;
      TTransport outputTransport = null;
      TProtocol inputProtocol = null;
      TProtocol outputProtocol = null;
      try {
        processor = processorFactory_.getProcessor(client_);
        inputTransport = inputTransportFactory_.getTransport(client_);
        outputTransport = outputTransportFactory_.getTransport(client_);
        inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
        outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
        while (processor.process(inputProtocol, outputProtocol)) {}
      } catch (TTransportException ttx) {
        // Assume the client died and continue silently
      } catch (TException tx) {
        tx.printStackTrace();
      } catch (Exception x) {
        x.printStackTrace();
      }
      
      if (inputTransport != null) {
        inputTransport.close();
      }
      
      if (outputTransport != null) {
        outputTransport.close();
      }
    }
  }
}
