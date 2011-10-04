package org.apache.accumulo.core.util;

import org.apache.accumulo.core.data.ByteSequence;

public class ByteArrayBackedCharSequence implements CharSequence {

	private byte[] data;
	private int offset;
	private int len;

	public ByteArrayBackedCharSequence(byte[] data, int offset, int len){
		this.data = data;
		this.offset = offset;
		this.len = len;
	}
	
	public ByteArrayBackedCharSequence(byte[] data){
		this(data, 0, data.length);
	}
	
	public ByteArrayBackedCharSequence(){
		this(null, 0, 0);
	}
	
	public void set(byte[] data, int offset, int len){
		this.data = data;
		this.offset = offset;
		this.len = len;
	}
	
	@Override
	public char charAt(int index) {
		return (char) (0xff & data[offset + index]);
	}

	@Override
	public int length() {
		return len;
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		return new ByteArrayBackedCharSequence(data, offset + start, end - start);
	}
	
	public String toString(){
		return new String(data, offset, len);
	}

	public void set(ByteSequence bs) {
		set(bs.getBackingArray(), bs.offset(), bs.length());
	}
}
