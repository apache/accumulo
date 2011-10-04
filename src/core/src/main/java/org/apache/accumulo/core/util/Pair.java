package org.apache.accumulo.core.util;

public class Pair<A,B> {
	A first;
	B second;
	
	public Pair(A f, B s){
		this.first = f;
		this.second = s;
	}
	
	private int hashCode(Object o){
		if(o == null)
			return 0;
		return o.hashCode();
	}
	
	@Override
	public int hashCode(){
		return hashCode(first) + hashCode(second);
	}
	
	private boolean equals(Object o1, Object o2){
		if(o1 == null || o2 == null)
			return o1 == o2;
		
		return o1.equals(o2);
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof Pair<?, ?>){
			Pair<?, ?> op = (Pair<?, ?>)o;
			return equals(first, op.first) && equals(second, op.second);
		}
		return false;
	}
	
	public A getFirst(){
		return first;
	}
	
	public B getSecond(){
		return second;
	}
	
	public String toString(){
		return "("+first+","+second+")";
	}
}

