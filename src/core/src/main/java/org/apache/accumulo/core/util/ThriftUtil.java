package org.apache.accumulo.core.util;

import java.net.InetSocketAddress;

import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import cloudtrace.instrument.thrift.TraceWrap;

public class ThriftUtil {
    //static private TProtocolFactory inputProtocolFactory = new TraceProtocol.Factory(true);
    //static private TProtocolFactory outputProtocolFactory = new TraceProtocol.Factory(false);
    static private TProtocolFactory  inputProtocolFactory = new TCompactProtocol.Factory();
    static private TProtocolFactory  outputProtocolFactory = new TCompactProtocol.Factory();
    static private TTransportFactory transportFactory = new TTransportFactory();
    
    static public <T extends TServiceClient> T createClient(TServiceClientFactory<T> factory, TTransport transport) {
        return TraceWrap.client(factory.getClient(inputProtocolFactory.getProtocol(transport), outputProtocolFactory.getProtocol(transport))); 
    }
    
    static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, InetSocketAddress address, AccumuloConfiguration conf) throws TTransportException {
        return createClient(factory, ThriftTransportPool.getInstance().getTransportWithDefaultTimeout(address, conf));
    }
    
    static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, Property property, AccumuloConfiguration configuration) throws TTransportException {
        int port = configuration.getPort(property);
        TTransport transport = ThriftTransportPool.getInstance().getTransport(address, port);
        return createClient(factory, transport);
    }
    
    static public <T extends TServiceClient> T getClient(TServiceClientFactory<T> factory, String address, Property property, Property timeoutProperty, AccumuloConfiguration configuration) throws TTransportException {
        int port = configuration.getPort(property);
        TTransport transport = ThriftTransportPool.getInstance().getTransport(address, port, configuration.getTimeInMillis(timeoutProperty));
        return createClient(factory, transport);
    }

    
    static public void returnClient(Object iface) { // Eew... the typing here is horrible
        if (iface != null) {
            TServiceClient client = (TServiceClient)iface;
            ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
        }
    }

    static public TabletClientService.Iface getTServerClient(String address, AccumuloConfiguration conf) throws TTransportException {
        return getClient(new TabletClientService.Client.Factory(), address, Property.TSERV_CLIENTPORT, Property.GENERAL_RPC_TIMEOUT, conf);
    }

    public static TTransportFactory transportFactory() {
        return transportFactory;
    }

    public static TProtocolFactory outputProtocolFactory() {
        return outputProtocolFactory;
    }

    public static TProtocolFactory inputProtocolFactory() {
        return inputProtocolFactory;
    }
}
