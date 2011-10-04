package org.apache.accumulo.server.master.balancer;

import java.util.Set;

public interface LoggerUser extends Comparable<LoggerUser> {
	
	Set<String> getLoggers();
}
