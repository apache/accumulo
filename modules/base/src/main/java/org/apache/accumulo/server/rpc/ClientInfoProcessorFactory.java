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
package org.apache.accumulo.server.rpc;

import org.apache.accumulo.core.rpc.TBufferedSocket;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets the address of a client in a ThreadLocal to allow for more informative log messages.
 */
public class ClientInfoProcessorFactory extends TProcessorFactory {
  private static final Logger log = LoggerFactory.getLogger(ClientInfoProcessorFactory.class);

  private final ThreadLocal<String> clientAddress;

  public ClientInfoProcessorFactory(ThreadLocal<String> clientAddress, TProcessor processor) {
    super(processor);
    this.clientAddress = clientAddress;
  }

  @Override
  public TProcessor getProcessor(TTransport trans) {
    if (trans instanceof TBufferedSocket) {
      TBufferedSocket tsock = (TBufferedSocket) trans;
      clientAddress.set(tsock.getClientString());
    } else if (trans instanceof TSocket) {
      TSocket tsock = (TSocket) trans;
      clientAddress.set(tsock.getSocket().getInetAddress().getHostAddress() + ":" + tsock.getSocket().getPort());
    } else {
      log.warn("Unable to extract clientAddress from transport of type {}", trans.getClass());
    }
    return super.getProcessor(trans);
  }
}
