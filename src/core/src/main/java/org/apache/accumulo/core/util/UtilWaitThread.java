package org.apache.accumulo.core.util;

import org.apache.log4j.Logger;

public class UtilWaitThread {
	private static final Logger log = Logger.getLogger(UtilWaitThread.class);
	
	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			log.error(e.getMessage(),e);
		}
	}
}
