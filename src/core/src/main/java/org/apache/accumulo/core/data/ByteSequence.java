package org.apache.accumulo.core.data;

import org.apache.hadoop.io.WritableComparator;

public abstract class ByteSequence implements Comparable<ByteSequence>{
	
	
	public abstract byte byteAt(int i);
	public abstract int length();
	
	public abstract ByteSequence subSequence(int start, int end);
	
	//may copy data
	public abstract byte[] toArray();
	
	public abstract boolean isBackedByArray();
	public abstract byte[] getBackingArray();
	public abstract int offset();
	
	public static int compareBytes(ByteSequence bs1, ByteSequence bs2) {
		
		int minLen = Math.min(bs1.length(), bs2.length());
		
		for(int i = 0; i < minLen; i++){
			int a = (bs1.byteAt(i) & 0xff);
			int b = (bs2.byteAt(i) & 0xff);
			
			if (a != b) {
				return a - b;
			}
		}
		
		return bs1.length() - bs2.length();
	}
	
	
	public int compareTo(ByteSequence obs) {
		if(isBackedByArray() && obs.isBackedByArray()){
			return WritableComparator.compareBytes(getBackingArray(), offset(), length(),
					obs.getBackingArray(), obs.offset(), obs.length());
		}
		
		return compareBytes(this, obs);
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof ByteSequence){
			ByteSequence obs = (ByteSequence) o;
			
			if(this == o)
				return true;
			
			if(length() != obs.length())
				return false;
			
			return compareTo(obs) == 0;
		}
		
		return false;
		
	}
	
	@Override
	public int hashCode(){
		int hash = 1;
		if(isBackedByArray()){
			byte[] data = getBackingArray();
			int end = offset()+length();
			for (int i = offset(); i < end; i++)
				hash = (31 * hash) + data[i];
		}else{
			for (int i = 0; i < length(); i++)
				hash = (31 * hash) + byteAt(i);
		}
		return hash;
	}
	
	
}
