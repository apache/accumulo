package org.apache.accumulo.core.file;

import java.io.IOException;

public class NoSuchMetaStoreException extends IOException {

	public NoSuchMetaStoreException(String msg, Throwable e) {
		super(msg, e);
	}

	public NoSuchMetaStoreException(String msg) {
		super(msg);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
