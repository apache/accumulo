package org.apache.accumulo.server.util;

import java.util.HashMap;

public class MapCounter<KT> {
	
	static class MutableLong {
		long l = 0l;
	}
	
	private HashMap<KT, MutableLong> map;
	
	public MapCounter(){
		map = new HashMap<KT, MutableLong>();
	}
	
	public long increment(KT key, long l){
		MutableLong ml = map.get(key);
		if(ml == null){
			ml = new MutableLong();
			map.put(key, ml);
		}
		
		ml.l += l;
		
		if(ml.l == 0){
			map.remove(key);
		}
		
		return ml.l;
	}
	
	public long decrement(KT key, long l){
		return increment(key, -1*l);
	}
	
	public boolean contains(KT key){
		return map.containsKey(key);
	}
	
	public long get(KT key){
		MutableLong ml = map.get(key);
		if(ml == null){
			return 0;
		}
		
		return ml.l;
	}
}
