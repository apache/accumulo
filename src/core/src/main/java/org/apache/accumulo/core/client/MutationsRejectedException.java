package org.apache.accumulo.core.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;


/**
 * Communicate the failed mutations of a BatchWriter back to the client.
 *
 */
public class MutationsRejectedException extends AccumuloException {
    private static final long serialVersionUID = 1L;

    private List<ConstraintViolationSummary> cvsl;
	private ArrayList<KeyExtent> af;
	private Collection<String> es;
	private int unknownErrors;
	
	/**
	 * @param cvsList list of constraint violations
	 * @param af authorization failures
	 * @param serverSideErrors server side errors
	 * @param unknownErrors number of unknown errors
	 */
	public MutationsRejectedException(List<ConstraintViolationSummary> cvsList, ArrayList<KeyExtent> af, Collection<String> serverSideErrors, int unknownErrors, Throwable cause) {
		super("# constraint violations : "+cvsList.size()+"  # authorization failures : "+af.size()+"  # server errors "+serverSideErrors.size()+" # exceptions "+unknownErrors, cause);
		this.cvsl = cvsList;
		this.af = af;
		this.es = serverSideErrors;
		this.unknownErrors = unknownErrors;
	}

	/**
	 * @return the internal list of constraint violations
	 */
	public List<ConstraintViolationSummary> getConstraintViolationSummaries(){
		return cvsl;
	}
	
	/**
	 * @return the internal list of authorization failures
	 */
	public List<KeyExtent> getAuthorizationFailures(){
		return af;
	}

	/**
	 * 
	 * @return A list of servers that had internal errors when mutations were written 
	 *         
	 */
	public Collection<String> getErrorServers(){
		return es;
	}
	
	/**
	 * 
	 * @return a count of unknown exceptions that occurred during processing
	 */
	public int getUnknownExceptions(){
		return unknownErrors;
	}
}
