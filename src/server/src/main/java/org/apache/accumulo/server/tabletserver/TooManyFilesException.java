package org.apache.accumulo.server.tabletserver;

import java.io.IOException;

public class TooManyFilesException extends IOException {

	private static final long serialVersionUID = 1L;

	public TooManyFilesException(String msg){
		super(msg);
	}
}
