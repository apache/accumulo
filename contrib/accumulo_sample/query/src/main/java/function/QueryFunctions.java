package function;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

public class QueryFunctions {
	
	protected static Logger log = Logger.getLogger(QueryFunctions.class);

	public static boolean between (String fieldValue, double left, double right) {
		try {
			Double value = Double.parseDouble(fieldValue);
			if (value >= left && value <= right)
				return true;
			return false;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}

	public static boolean between (String fieldValue, long left, long right) {
		try {
			Long value = Long.parseLong(fieldValue);
			if (value >= left && value <= right)
				return true;
			return false;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}
	
	public static Number abs (String fieldValue) {
		Number retval = null;
		try {
			Number value = NumberUtils.createNumber(fieldValue);
			if (null == value)
				retval = (Number) Integer.MIN_VALUE;
			else if (value instanceof Long)
				retval = Math.abs(value.longValue());
			else if (value instanceof Double)
				retval = Math.abs(value.doubleValue());
			else if (value instanceof Float)
				retval = Math.abs(value.floatValue());
			else if (value instanceof Integer)
				retval = Math.abs(value.intValue());
		} catch (NumberFormatException nfe) {
			return (Number) Integer.MIN_VALUE;
		}
		return retval;
	}
	
}
