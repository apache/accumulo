package org.apache.accumulo.core.util;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;

public class Encoding {

	public static String encodeAsBase64FileName(Text data) {
    	String encodedRow = new String(Base64.encodeBase64(TextUtil.getBytes(data)));
    	encodedRow = encodedRow.replace('/', '_').replace('+', '-');
    	
    	int index = encodedRow.length() - 1;
    	while(index >=0 && encodedRow.charAt(index) == '=')
    		index--;
    	
    	encodedRow = encodedRow.substring(0, index+1);
		return encodedRow;
	}

	public static byte[] decodeBase64FileName(String node) {
		while(node.length() % 4 != 0)
			node += "=";
		
		node = node.replace('_', '/').replace('-', '+');
		
		return Base64.decodeBase64(node.getBytes());
	}
	    
}
