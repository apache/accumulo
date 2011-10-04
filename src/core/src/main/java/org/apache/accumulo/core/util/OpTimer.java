package org.apache.accumulo.core.util;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class OpTimer {
	private Logger log;
	private Level level;
	private long t1;
	private long opid;
	private static AtomicLong nextOpid = new AtomicLong();
	
	public OpTimer(Logger log, Level level){
		this.log = log;
		this.level = level;
	}
	
	public OpTimer start(String msg){
		opid = nextOpid.getAndIncrement();
		if(log.isEnabledFor(level)) log.log(level, "tid="+Thread.currentThread().getId()+" oid="+opid+"  "+msg);
		t1 = System.currentTimeMillis();
		return this;
	}
	
	public void stop(String msg){
		if(log.isEnabledFor(level)) {
			long t2 = System.currentTimeMillis();
			String duration = String.format("%.3f secs", (t2 -t1)/1000.0);
			msg = msg.replace("%DURATION%", duration);
			log.log(level, "tid="+Thread.currentThread().getId()+" oid="+opid+"  "+msg);
		}
	}
}
