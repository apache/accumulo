package org.apache.accumulo.server.test;

public class FastFormat {
	//this 7 to 8 times faster than String.format("%s%06d",prefix, num)
	public static byte[] toZeroPaddedString(long num, int width, int radix, byte[] prefix){
		byte ret[] = new byte[width+prefix.length];
		if(toZeroPaddedString(ret, 0, num, width, radix, prefix) != ret.length)
			throw new RuntimeException(" Did not format to expected width "+num+" "+width+" "+radix+" "+new String(prefix));
		return ret;
	}
	
	public static int toZeroPaddedString(byte output[], int outputOffset, long num, int width, int radix, byte[] prefix){
		if(num < 0)
			throw new IllegalArgumentException();
		
		String s = Long.toString(num, radix);
		
		int index = outputOffset;
		
		for(int i = 0; i < prefix.length; i++){
			output[index++] = prefix[i];
		}
		
		int end = width - s.length() + index;
		
		while(index < end)
			output[index++] = '0';
		
		for(int i = 0; i < s.length(); i++){
			output[index++] = (byte) s.charAt(i);
		}
		
		return index - outputOffset;
	}
}
