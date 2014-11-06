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
package org.apache.accumulo.core.util;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.net.HostAndPort;

public class ThriftUtil {
  private static final Logger log = Logger.getLogger(ThriftUtil.class);

  public static class TraceProtocol extends TCompactProtocol {
    private Span span = null;

    @Override
    public void writeMessageBegin(TMessage message) throws TException {
      span = Trace.start("client:" + message.name);
      super.writeMessageBegin(message);
    }

    @Override
    public void writeMessageEnd() throws TException {
      super.writeMessageEnd();
      span.stop();
    }

    public TraceProtocol(TTransport transport) {
      super(transport);
    }
  }

  public static class TraceProtocolFactory extends TCompactProtocol.Factory {
    private static final long serialVersionUID = 1L;

    @Override
    public TProtocol getProtocol(TTransport trans) {
      return new TraceProtocol(trans);
    }
  }

  static private TProtocolFactory protocolFactory = new TraceProtocolFactory();
  static private TTransportFactory transportFactory = new TFramedTransport.Factory(Integer.MAX_VALUE);

  static public <T extends TServiceClient> T createClient(TServiceClientFactory<T> factory, TTransport transport) {
    return factory.getClient(protocolFactory.getProtocol(transport), protocolFactory.getProtocol(transport));
  }

  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, HostAndPort address, AccumuloConfiguration conf)
      throws TTransportException {
    return createClient(factory, ThriftTransportPool.getInstance().getTransportWithDefaultTimeout(address, conf));
  }

  static public <T extends TServiceClient> T getClientNoTimeout(TServiceClientFactory<T> factory, String address, AccumuloConfiguration configuration)
      throws TTransportException {
    return getClient(factory, address, 0, configuration);
  }

  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, Property timeoutProperty,
      AccumuloConfiguration configuration) throws TTransportException {
    long timeout = configuration.getTimeInMillis(timeoutProperty);
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, timeout, SslConnectionParams.forClient(configuration));
    return createClient(factory, transport);
  }

  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, long timeout, AccumuloConfiguration configuration)
      throws TTransportException {
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, timeout, SslConnectionParams.forClient(configuration));
    return createClient(factory, transport);
  }

  static public void returnClient(TServiceClient iface) { // Eew... the typing here is horrible
    if (iface != null) {
      ThriftTransportPool.getInstance().returnTransport(iface.getInputProtocol().getTransport());
    }
  }

  static public TabletClientService.Client getTServerClient(String address, AccumuloConfiguration conf) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, Property.GENERAL_RPC_TIMEOUT, conf);
  }

  static public TabletClientService.Client getTServerClient(String address, AccumuloConfiguration conf, long timeout) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, timeout, conf);
  }

  public static void execute(String address, AccumuloConfiguration conf, ClientExec<TabletClientService.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    while (true) {
      TabletClientService.Client client = null;
      try {
        exec.execute(client = getTServerClient(address, conf));
        break;
      } catch (TTransportException tte) {
        log.debug("getTServerClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          returnClient(client);
      }
    }
  }

  public static <T> T execute(String address, AccumuloConfiguration conf, ClientExecReturn<T,TabletClientService.Client> exec) throws AccumuloException,
      AccumuloSecurityException {
    while (true) {
      TabletClientService.Client client = null;
      try {
        return exec.execute(client = getTServerClient(address, conf));
      } catch (TTransportException tte) {
        log.debug("getTServerClient request failed, retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (Exception e) {
        throw new AccumuloException(e);
      } finally {
        if (client != null)
          returnClient(client);
      }
    }
  }

  /**
   * create a transport that is not pooled
   */
  public static TTransport createTransport(HostAndPort address, AccumuloConfiguration conf) throws TException {
    return createClientTransport(address, (int) conf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT), SslConnectionParams.forClient(conf));
  }

  public static TTransportFactory transportFactory() {
    return transportFactory;
  }

  private final static Map<Integer,TTransportFactory> factoryCache = new HashMap<Integer,TTransportFactory>();

  synchronized public static TTransportFactory transportFactory(int maxFrameSize) {
    TTransportFactory factory = factoryCache.get(maxFrameSize);
    if (factory == null) {
      factory = new TFramedTransport.Factory(maxFrameSize);
      factoryCache.put(maxFrameSize, factory);
    }
    return factory;
  }

  synchronized public static TTransportFactory transportFactory(long maxFrameSize) {
    if (maxFrameSize > Integer.MAX_VALUE || maxFrameSize < 1)
      throw new RuntimeException("Thrift transport frames are limited to " + Integer.MAX_VALUE);
    return transportFactory((int) maxFrameSize);
  }

  public static TProtocolFactory protocolFactory() {
    return protocolFactory;
  }

  public static TServerSocket getServerSocket(int port, int timeout, InetAddress address, SslConnectionParams params) throws TTransportException {
    if (params.useJsse()) {
      return TSSLTransportFactory.getServerSocket(port, timeout, params.isClientAuth(), address);
    } else {
      return TSSLTransportFactory.getServerSocket(port, timeout, address, params.getTTransportParams());
    }
  }

  public static TTransport createClientTransport(HostAndPort address, int timeout, SslConnectionParams sslParams) throws TTransportException {
    boolean success = false;
    TTransport transport = null;
    try {
      if (sslParams != null) {
        // TSSLTransportFactory handles timeout 0 -> forever natively
        if (sslParams.useJsse()) {
          transport = TSSLTransportFactory.getClientSocket(address.getHostText(), address.getPort(), timeout);
        } else {
          transport = TSSLTransportFactory.getClientSocket(address.getHostText(), address.getPort(), timeout, sslParams.getTTransportParams());
        }
        // TSSLTransportFactory leaves transports open, so no need to open here
      } else if (timeout == 0) {
        transport = new TSocket(address.getHostText(), address.getPort());
        transport.open();
      } else {
        try {
          transport = TTimeoutTransport.create(address, timeout);
        } catch (IOException ex) {
          throw new TTransportException(ex);
        }
        transport.open();
      }
      transport = ThriftUtil.transportFactory().getTransport(transport);
      success = true;
    } finally {
      if (!success && transport != null) {
        transport.close();
      }
    }
    return transport;
  }
}
