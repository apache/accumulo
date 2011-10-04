package org.apache.accumulo.core.client;

/**
 * A generic Accumulo Exception for general accumulo failures.
 * 
 */
public class AccumuloException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * @param why is the reason for the error being thrown
	 */
	public AccumuloException(String why)
	{ super(why); }

	/**
	 * @param cause is the exception that this exception wraps
	 */
	public AccumuloException(Throwable cause)
	{ super(cause); }

	/**
	 * @param why is the reason for the error being thrown
	 * @param cause is the exception that this exception wraps
	 */
	public AccumuloException(String why, Throwable cause)
	{ super(why, cause); }

}
