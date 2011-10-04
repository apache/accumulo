package org.apache.accumulo.core.util;

import java.net.InetSocketAddress;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ThriftTransportPool;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.log4j.Logger;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import cloudtrace.instrument.thrift.TraceWrap;

public class ThriftUtil {
    private static final Logger log = Logger.getLogger(ThriftUtil.class);
    
    static private TProtocolFactory  protocolFactory = new TCompactProtocol.Factory();
    static private TTransportFactory transportFactory = new TFramedTransport.Factory();
    
    static public <T extends TServiceClient> T createClient(TServiceClientFactory<T> factory, TTransport transport) {
        return TraceWrap.client(factory.getClient(protocolFactory.getProtocol(transport), protocolFactory.getProtocol(transport))); 
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
    
    public static void execute(String address, AccumuloConfiguration conf, ClientExec<TabletClientService.Iface> exec) throws AccumuloException, AccumuloSecurityException {
        while (true) {
            TabletClientService.Iface client = null;
            try {
                exec.execute(client = getTServerClient(address, conf));
                break;
            } catch(TTransportException tte){
                log.debug("getTServerClient request failed, retrying ... ", tte);
                UtilWaitThread.sleep(100);
            } catch (ThriftSecurityException e) {
                throw new AccumuloSecurityException(e.user, e.code, e);
            } catch(Exception e){
                throw new AccumuloException(e);
            } finally {
                if (client != null) 
                    returnClient(client);
            }
        }
    }
    
    public static <T> T execute(String address, AccumuloConfiguration conf, ClientExecReturn<T, TabletClientService.Iface> exec) throws AccumuloException, AccumuloSecurityException {
        while (true) {
            TabletClientService.Iface client = null;
            try {
                return exec.execute(client = getTServerClient(address, conf));
            } catch(TTransportException tte){
                log.debug("getTServerClient request failed, retrying ... ", tte);
                UtilWaitThread.sleep(100);
            } catch (ThriftSecurityException e) {
                throw new AccumuloSecurityException(e.user, e.code, e);
            } catch(Exception e){
                throw new AccumuloException(e);
            } finally {
                if (client != null) 
                    returnClient(client);
            }
        }
    }
    
    public static TTransportFactory transportFactory() {
        return transportFactory;
    }

    public static TProtocolFactory protocolFactory() {
        return protocolFactory;
    }
}
