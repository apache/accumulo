package org.apache.accumulo.core.client.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;


public class MockInstance implements Instance {
    
    static final String genericAddress = "localhost:1234"; 
    static final Map<String,MockAccumulo> instances = new HashMap<String,MockAccumulo>();
    MockAccumulo acu;
    String instanceName;
    
    public MockInstance() {
    	acu = new MockAccumulo();
    	instanceName = "mock-instance";
    }
    
    public MockInstance(String instanceName) {
    	synchronized(instances) {
	    	if (instances.containsKey(instanceName))
	    		acu = instances.get(instanceName);
	    	else
	    		instances.put(instanceName, acu = new MockAccumulo());
    	}
    	this.instanceName = instanceName;
    }
    
    @Override
    public String getRootTabletLocation() {
        return genericAddress;
    }

    @Override
    public List<String> getMasterLocations() {
        return Collections.singletonList(genericAddress);
    }

    @Override
    public String getInstanceID() {
        return "mock-instance-id";
    }

    @Override
    public String getInstanceName() {
        return instanceName;
    }

    @Override
    public String getZooKeepers() {
        return "localhost";
    }

    @Override
    public int getZooKeepersSessionTimeOut() {
        return 30*1000;
    }
    
    @Override
    public Connector getConnector(String user, byte[] pass) throws AccumuloException, AccumuloSecurityException {
        return new MockConnector(user, acu);
    }

    @Override
	public Connector getConnector(String user, CharSequence pass) throws AccumuloException, AccumuloSecurityException {
		return getConnector(user, TextUtil.getBytes(new Text(pass.toString())));
	}
    
    AccumuloConfiguration conf = null;
    
	@Override
	public AccumuloConfiguration getConfiguration() {
		if(conf == null)
			conf = AccumuloConfiguration.getDefaultConfiguration();
		return conf;
	}

	@Override
	public void setConfiguration(AccumuloConfiguration conf) {
		this.conf = conf;
	}
}
