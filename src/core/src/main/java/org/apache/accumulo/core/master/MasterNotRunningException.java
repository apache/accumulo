package org.apache.accumulo.core.master;

public class MasterNotRunningException extends Exception {

	/**
	 * eclipse generated this ... 
	 */
	private static final long serialVersionUID = 1L;
	private String message;
	public MasterNotRunningException(String msg) {
		message = msg;
	}

	public String getMessage() {
		return message;
	}

}


