/**
 * 
 */
package org.apache.accumulo.core.util;

import java.io.Serializable;
import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]>, Serializable
{
    private static final long serialVersionUID = 1L;

    @Override
	public int compare(byte[] o1, byte[] o2) {
		
		int minLen = Math.min(o1.length, o2.length);
		
		for(int i = 0; i < minLen; i++){
			int a = (o1[i] & 0xff);
			int b = (o2[i] & 0xff);
			
			if (a != b) {
				return a - b;
			}
		}
		
		return o1.length - o2.length;
	}
}