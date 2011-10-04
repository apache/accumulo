package org.apache.accumulo.core.client.impl;

import java.util.List;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.log4j.Logger;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;


public class MasterClient
{
	private static final Logger log = Logger.getLogger(MasterClient.class);
	
	public static synchronized MasterClientService.Iface getConnection(Instance instance)
	throws TTransportException
	{
		ArgumentChecker.notNull(instance);
		return getConnection(instance, true);
	}

	public static synchronized MasterClientService.Iface getConnection(Instance instance, boolean retry)
	throws TTransportException
	{
	    ArgumentChecker.notNull(instance);

	    do {

	        List<String> locations = instance.getMasterLocations();

	        while(locations.size() == 0){
	            log.debug("No masters, will retry...");
	            UtilWaitThread.sleep(250);
	            locations = instance.getMasterLocations();
	        }

	        String master = locations.get(0);
	        int portHint = instance.getConfiguration().getPort(Property.MASTER_CLIENTPORT);

	        try{
	            // Master requests can take a long time: don't ever time out
	            MasterClientService.Iface client = ThriftUtil.getClient(new MasterClientService.Client.Factory(), master, Property.MASTER_CLIENTPORT, instance.getConfiguration());
	            return client;
	        }catch(TTransportException tte){
	            log.debug("Failed to connect to master="+master+" portHint="+portHint+ (retry?", will retry... ":""), tte);
	        }

	        UtilWaitThread.sleep(250);
	    } while (retry)
	        ;
	    return null;
	}


	public static void close(MasterClientService.Iface iface)
	{
	    TServiceClient client = (TServiceClient)iface;
		if (client != null
				&& client.getInputProtocol() != null
				&& client.getInputProtocol().getTransport() != null)
		{
			ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
		}else{
			log.debug("Attempt to close null connection to the master", new Exception());
		}
	}
}
