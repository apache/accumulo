package org.apache.accumulo.core.util;

/**
 * This class provides methods to check arguments of a variable number for null values, or anything
 * else that might be required on a routine basis. These methods should be used for early failures
 * as close to the end user as possible, so things do not fail later on the server side, when they
 * are harder to debug.
 * 
 * Methods are created for a specific number of arguments, due to the poor performance of array
 * allocation for varargs methods.
 */
public class ArgumentChecker
{
	private static final String NULL_ARG_MSG = "argument was null";
	
	public static final void notNull(final Object arg1)
	{
		if (arg1 == null)
			throw new IllegalArgumentException(NULL_ARG_MSG);
	}

	public static final void notNull(final Object arg1, final Object arg2)
	{
		if (arg1 == null || arg2 == null)
			throw new IllegalArgumentException(NULL_ARG_MSG);
	}

	public static final void notNull(final Object arg1, final Object arg2, final Object arg3)
	{
		if (arg1 == null || arg2 == null || arg3 == null)
			throw new IllegalArgumentException(NULL_ARG_MSG);
	}

	public static final void notNull(final Object arg1, final Object arg2, final Object arg3, final Object arg4)
	{
		if (arg1 == null || arg2 == null || arg3 == null || arg4 == null)
			throw new IllegalArgumentException(NULL_ARG_MSG);
	}
	
	public static final void notNull(final Object[] args)
	{
		if (args == null)
			throw new IllegalArgumentException(NULL_ARG_MSG);
			
		for (Object arg : args)
			if (arg == null)
				throw new IllegalArgumentException(NULL_ARG_MSG);
	}
}
