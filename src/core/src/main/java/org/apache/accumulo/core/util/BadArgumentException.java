package org.apache.accumulo.core.util;

import java.util.regex.PatternSyntaxException;

public final class BadArgumentException extends PatternSyntaxException
{
	private static final long serialVersionUID = 1L;
	public BadArgumentException(String desc, String badarg, int index) { super(desc, badarg, index); }
}
