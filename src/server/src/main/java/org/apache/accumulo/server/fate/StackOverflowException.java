package org.apache.accumulo.server.fate;

public class StackOverflowException extends Exception {

	public StackOverflowException(String msg) {
		super(msg);
	}

	private static final long serialVersionUID = 1L;
	
}
