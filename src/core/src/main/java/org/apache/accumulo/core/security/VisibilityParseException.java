package org.apache.accumulo.core.security;

import java.text.ParseException;

public class VisibilityParseException extends ParseException
{
	private static final long serialVersionUID = 1L;
	private String visibility;
	
	public VisibilityParseException(String reason, byte[] visibility, int errorOffset)
	{
		super(reason, errorOffset);
		this.visibility = new String(visibility);
	}

	@Override
	public String getMessage()
	{
		return super.getMessage() + " in string '"+visibility+"' at position " + super.getErrorOffset();
	}
}
