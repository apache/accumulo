package org.apache.accumulo.core.iterators;

public class IterationInterruptedException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;
	
	public IterationInterruptedException(){
		super();
	}
	
	public IterationInterruptedException(String msg) {
		super(msg);
	}	
}
