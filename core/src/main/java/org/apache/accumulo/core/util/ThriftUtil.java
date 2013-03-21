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
import java.net.InetSocketAddress;
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
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;


public class ThriftUtil {
  private static final Logger log = Logger.getLogger(ThriftUtil.class);

  public static class TraceProtocol extends TCompactProtocol {

    @Override
    public void writeMessageBegin(TMessage message) throws TException {
      Trace.start("client:" + message.name);
      super.writeMessageBegin(message);
    }

    @Override
    public void writeMessageEnd() throws TException {
      super.writeMessageEnd();
      Span currentTrace = Trace.currentTrace();
      if (currentTrace != null)
        currentTrace.stop();
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
  static private TTransportFactory transportFactory = new TFramedTransport.Factory();
  
  static public <T extends TServiceClient> T createClient(TServiceClientFactory<T> factory, TTransport transport) {
    return factory.getClient(protocolFactory.getProtocol(transport), protocolFactory.getProtocol(transport));
  }
  
  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, InetSocketAddress address, AccumuloConfiguration conf)
      throws TTransportException {
    return createClient(factory, ThriftTransportPool.getInstance().getTransportWithDefaultTimeout(address, conf));
  }
  
  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, Property property, AccumuloConfiguration configuration)
      throws TTransportException {
    int port = configuration.getPort(property);
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, port);
    return createClient(factory, transport);
  }
  
  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, Property property, Property timeoutProperty,
      AccumuloConfiguration configuration) throws TTransportException {
    return getClient(factory, address, property, configuration.getTimeInMillis(timeoutProperty), configuration);
  }
  
  static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, Property property, long timeout,
      AccumuloConfiguration configuration) throws TTransportException {
    int port = configuration.getPort(property);
    TTransport transport = ThriftTransportPool.getInstance().getTransport(address, port, timeout);
    return createClient(factory, transport);
  }
  
  static public void returnClient(TServiceClient iface) { // Eew... the typing here is horrible
    if (iface != null) {
      ThriftTransportPool.getInstance().returnTransport(iface.getInputProtocol().getTransport());
    }
  }
  
  static public TabletClientService.Client getTServerClient(String address, AccumuloConfiguration conf) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, Property.TSERV_CLIENTPORT, Property.GENERAL_RPC_TIMEOUT, conf);
  }
  
  static public TabletClientService.Client getTServerClient(String address, AccumuloConfiguration conf, long timeout) throws TTransportException {
    return getClient(new TabletClientService.Client.Factory(), address, Property.TSERV_CLIENTPORT, timeout, conf);
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
  public static TTransport createTransport(String address, int port, AccumuloConfiguration conf) throws TException {
    TTransport transport = null;
    
    try {
      transport = TTimeoutTransport.create(org.apache.accumulo.core.util.AddressUtil.parseAddress(address, port),
          conf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT));
      transport = ThriftUtil.transportFactory().getTransport(transport);
      transport.open();
      TTransport tmp = transport;
      transport = null;
      return tmp;
    } catch (IOException ex) {
      throw new TTransportException(ex);
    } finally {
      if (transport != null)
        transport.close();
    }
    

  }

  /**
   * create a transport that is not pooled
   */
  public static TTransport createTransport(InetSocketAddress address, AccumuloConfiguration conf) throws TException {
    return createTransport(address.getAddress().getHostAddress(), address.getPort(), conf);
  }

  public static TTransportFactory transportFactory() {
    return transportFactory;
  }
  
  private final static Map<Integer,TTransportFactory> factoryCache = new HashMap<Integer,TTransportFactory>();
  synchronized public static TTransportFactory transportFactory(int maxFrameSize) {
    TTransportFactory factory = factoryCache.get(maxFrameSize);
    if(factory == null)
    {
      factory = new TFramedTransport.Factory(maxFrameSize);
      factoryCache.put(maxFrameSize,factory);
    }
    return factory;
  }

  synchronized public static TTransportFactory transportFactory(long maxFrameSize) {
    if(maxFrameSize > Integer.MAX_VALUE || maxFrameSize < 1)
      throw new RuntimeException("Thrift transport frames are limited to "+Integer.MAX_VALUE);
    return transportFactory((int)maxFrameSize);
  }

  public static TProtocolFactory protocolFactory() {
    return protocolFactory;
  }
}
