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
package org.apache.accumulo.tracer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.apache.accumulo.tracer.thrift.SpanReceiver.Client;
import org.apache.htrace.HTraceConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Send Span data to a destination using thrift.
 */
public class SendSpansViaThrift extends AsyncSpanReceiver<String,Client> {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SendSpansViaThrift.class);

  private static final String THRIFT = "thrift://";

  // Visible for testing
  SendSpansViaThrift() {}

  public SendSpansViaThrift(HTraceConfiguration conf) {
    super(conf);
  }

  @Override
  protected Client createDestination(String destination) throws Exception {
    if (destination == null)
      return null;
    try {
      int portSeparatorIndex = destination.lastIndexOf(':');
      String host = destination.substring(0, portSeparatorIndex);
      int port = Integer.parseInt(destination.substring(portSeparatorIndex + 1));
      log.debug("Connecting to {}:{}", host, port);
      InetSocketAddress addr = new InetSocketAddress(host, port);
      Socket sock = new Socket();
      sock.connect(addr);
      TTransport transport = new TSocket(sock);
      TProtocol prot = new TBinaryProtocol(transport);
      return new Client(prot);
    } catch (IOException ex) {
      log.trace("{}", ex, ex);
      return null;
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
      return null;
    }
  }

  @Override
  protected void send(Client client, RemoteSpan s) throws Exception {
    if (client != null) {
      try {
        client.span(s);
      } catch (Exception ex) {
        client.getInputProtocol().getTransport().close();
        throw ex;
      }
    }
  }

  private static final String DEST = "dest";

  @Override
  protected String getSpanKey(Map<String,String> data) {
    String dest = data.get(DEST);
    if (dest != null && dest.startsWith(THRIFT)) {
      String hostAddress = dest.substring(THRIFT.length());
      String[] hostAddr = hostAddress.split(":", 2);
      if (hostAddr.length == 2) {
        return hostAddress;
      }
    }
    return null;
  }

}
