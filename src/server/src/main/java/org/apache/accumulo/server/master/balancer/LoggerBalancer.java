package org.apache.accumulo.server.master.balancer;

import java.util.List;
import java.util.Map;

public interface LoggerBalancer {
	/**
	 * Assign loggers to tablet servers
	 * 
	 * @param current
	 *            The loggers assigned to each tablet server
	 * @param loggers
	 *            The list of current loggers
	 * @param assignmentsOut
	 *            the assignments to give to tablet servers
	 */
	public abstract void balance(List<LoggerUser> current,
			List<String> loggers,
			Map<LoggerUser, List<String>> assignmentsOut,
			int loggersPerServer);

}
