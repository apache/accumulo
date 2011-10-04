package org.apache.accumulo.core.data;


public class ArrayByteSequence extends ByteSequence {

	protected byte data[];
	protected int offset;
	protected int length;
	
	public ArrayByteSequence(byte data[]){
		this.data = data;
		this.offset = 0;
		this.length = data.length;
	}
	
	public ArrayByteSequence(byte data[], int offset, int length) {
		
		if(offset < 0 || offset > data.length || length < 0 || (offset+length) > data.length){
			throw new IllegalArgumentException(" Bad offset and/or length data.length = "+data.length+" offset = "+offset+" length = "+length);
		}
		
		this.data = data;
		this.offset = offset;
		this.length = length;
		
	}
	
	public ArrayByteSequence(String s) {
		this(s.getBytes());
	}

	@Override
	public byte byteAt(int i) {
	
		
		if(i < 0){
			throw new IllegalArgumentException("i < 0, "+i);
		}
		
		if(i >= length){
			throw new IllegalArgumentException("i >= length, "+i+" >= "+length);
		}
		
		return data[offset + i];
	}
	

	@Override
	public byte[] getBackingArray() {
		return data;
	}

	@Override
	public boolean isBackedByArray() {
		return true;
	}

	@Override
	public int length() {
		return length;
	}

	@Override
	public int offset() {
		return offset;
	}

	@Override
	public ByteSequence subSequence(int start, int end) {
		
		if(start > end || start < 0 || end > length){
			throw new IllegalArgumentException("Bad start and/end start = "+start+" end="+end+" offset="+offset+" length="+length);
		}
		
		return new ArrayByteSequence(data, offset+start, end - start);
	}

	@Override
	public byte[] toArray() {
		if(offset == 0 && length == data.length)
			return data;
		
		byte[] copy = new byte[length];
		System.arraycopy(data, offset, copy, 0, length);
		return copy;
	}
	
	public String toString(){
		return new String(data, offset, length);
	}
}
