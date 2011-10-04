package org.apache.accumulo.core.data;

import org.apache.hadoop.io.BinaryComparable;

public class ComparableBytes extends BinaryComparable {

	public byte[] data;

	public ComparableBytes(byte[] b){
		this.data = b;
	}
	
	public byte[] getBytes() {
		return data;
	}

	@Override
	public int getLength() {
		return data.length;
	}

}
