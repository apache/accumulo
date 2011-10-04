package org.apache.accumulo.server.fate;

import java.io.Serializable;

/**
 * Repeatable persisted operation
 *
 */
public interface Repo<T> extends Serializable {
	long isReady(long tid, T environment) throws Exception;
	Repo<T> call(long tid, T environment) throws Exception;
	void undo(long tid, T environment) throws Exception;
    String getDescription();
    
    //this allows the last fate op to return something to the user
    String getReturn();
}