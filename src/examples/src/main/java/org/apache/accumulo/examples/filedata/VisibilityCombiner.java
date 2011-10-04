package org.apache.accumulo.examples.filedata;

import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;


public class VisibilityCombiner {
	
	private TreeSet<String> visibilities = new TreeSet<String>();
	
	void add(ByteSequence cv){
		if(cv.length() == 0)
			return;
		
		
		int depth = 0;
		int offset = 0;
		
		for(int i = 0; i < cv.length(); i++){
			switch(cv.byteAt(i)){
			case '(':
				depth++;
				break;
			case ')':
				depth--;
				if(depth < 0)
					throw new IllegalArgumentException("Invalid vis "+cv);
				break;
			case '|':
				if(depth == 0){
					insert(cv.subSequence(offset, i));
					offset = i+1;
				}
				
				break;
			}
		}
		
		insert(cv.subSequence(offset, cv.length()));
		
		if(depth != 0)
			throw new IllegalArgumentException("Invalid vis "+cv);
		
	}
	
	private void insert(ByteSequence cv) {
		for(int i = 0; i < cv.length(); i++){
			
		}
		
		String cvs = cv.toString();
		
		if(cvs.charAt(0) != '(')
			cvs = "("+cvs+")";
		else{
			int depth = 0;
			int depthZeroCloses = 0;
			for(int i = 0; i < cv.length(); i++){
				switch(cv.byteAt(i)){
				case '(':
					depth++;
					break;
				case ')':
					depth--;
					if(depth == 0)
						depthZeroCloses++;
					break;
				}
			}
			
			if(depthZeroCloses > 1)
				cvs = "("+cvs+")";
		}
			
		
		visibilities.add(cvs);
	}

	byte[] get(){
		StringBuilder sb = new StringBuilder();
		String sep = "";
		for(String cvs : visibilities){
			sb.append(sep);
			sep = "|";
			sb.append(cvs);
		}
		
		return sb.toString().getBytes();
	}
}
