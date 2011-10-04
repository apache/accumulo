package org.apache.accumulo.server.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class NamingThreadFactory implements ThreadFactory {

	private ThreadFactory dtf = Executors.defaultThreadFactory();
	private int threadNum = 1;
	private String name;
	
	public NamingThreadFactory(String name){
		this.name = name;
	}
	
	public Thread newThread(Runnable r) {
		Thread thread = dtf.newThread(r);
		thread.setName(name+" "+threadNum++);
		return thread;
	}
	
}