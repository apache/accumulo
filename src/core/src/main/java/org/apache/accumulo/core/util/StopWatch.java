package org.apache.accumulo.core.util;

import java.util.EnumMap;

public class StopWatch<K extends Enum<K>> {
	EnumMap<K, Long> startTime;
	EnumMap<K, Long> totalTime;
	
	public StopWatch(Class<K> k){
		startTime = new EnumMap<K, Long>(k);
		totalTime = new EnumMap<K, Long>(k);
	}
	
	public synchronized void start(K timer) {
		if(startTime.containsKey(timer)){
			throw new IllegalStateException(timer+" already started");
		}
		startTime.put(timer, System.currentTimeMillis());
	}
	
	public synchronized void stopIfActive(K timer) {
		if(startTime.containsKey(timer))
			stop(timer);
	}
	
	public synchronized void stop(K timer) {
		
		Long st = startTime.get(timer);
		
		if(st == null){
			throw new IllegalStateException(timer+" not started");
		}
		
		Long existingTime = totalTime.get(timer);
		if(existingTime == null)
			existingTime = 0L;
		
		totalTime.put(timer, existingTime + (System.currentTimeMillis() - st));
		startTime.remove(timer);
	}
	
	public synchronized void reset(K timer) {
		totalTime.remove(timer);
	}
	
	public synchronized long get(K timer) {
		Long existingTime = totalTime.get(timer);
		if(existingTime == null)
			existingTime = 0L;
		return existingTime;
	}
	
	public synchronized double getSecs(K timer) {
		Long existingTime = totalTime.get(timer);
		if(existingTime == null)
			existingTime = 0L;
		return existingTime/1000.0;
	}
	
	public synchronized void print() {
		for(K timer : totalTime.keySet()){
			System.out.printf("%20s : %,6.4f secs\n", timer.toString(), get(timer)/1000.0);
		}
	}

	
}


