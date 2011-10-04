package org.apache.accumulo.server.tabletserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.Key;


class MemKey extends Key {
	
	int mutationCount;
	
	public MemKey(byte[] row, byte[] cf, byte[] cq, byte[] cv, long ts, boolean del, boolean copy, int mc) {
		super(row, cf, cq, cv, ts, del, copy);
		this.mutationCount = mc;
	}

	public MemKey() {
		super();
		this.mutationCount = Integer.MAX_VALUE;
	}
	
	public MemKey(Key key, int mc) {
		super(key);
		this.mutationCount = mc;
	}

	public String toString(){
		return super.toString()+" mc="+mutationCount;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException{
	    return super.clone();
	}
	
	@Override
	public void write(DataOutput out) throws IOException{
		super.write(out);
		out.writeInt(mutationCount);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException{
		super.readFields(in);
		mutationCount = in.readInt();
	}
	
	@Override
	public int compareTo(Key k){
		
		
		int cmp = super.compareTo(k);
		
		if(cmp == 0 && k instanceof MemKey){
			cmp = ((MemKey)k).mutationCount - mutationCount;
		}
		
		return cmp;
	}
	
}
