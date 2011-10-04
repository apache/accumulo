package org.apache.accumulo.server.master.balancer;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.server.master.state.TServerInstance;


public class TServerUsesLoggers implements LoggerUser {

	private TabletServerStatus status;
	private TServerInstance instance;
	
	public TServerUsesLoggers(TServerInstance instance, TabletServerStatus status) {
		this.instance = instance;
		this.status = status;
	}
	
	@Override
	public Set<String> getLoggers() {
		return Collections.unmodifiableSet(status.loggers);
	}

	@Override
	public int compareTo(LoggerUser o) {
	    if (o instanceof TServerUsesLoggers)
	        return instance.compareTo( ((TServerUsesLoggers)o).instance);
	    return -1;
	}

	@Override
	public int hashCode() {
		return instance.hashCode();
	}
	
	public TServerInstance getInstance() { return instance; }
}
