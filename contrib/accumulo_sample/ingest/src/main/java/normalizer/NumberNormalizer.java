package normalizer;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.lucene.util.NumericUtils;


public class NumberNormalizer implements Normalizer {

	public String normalizeFieldValue(String field, Object value) {
		if (NumberUtils.isNumber(value.toString())) {
			Number n = NumberUtils.createNumber(value.toString());
			if (n instanceof Integer)
				return NumericUtils.intToPrefixCoded((Integer) n);
			else if (n instanceof Long)
				return NumericUtils.longToPrefixCoded((Long) n);
			else if (n instanceof Float)
				return NumericUtils.floatToPrefixCoded((Float) n);
			else if (n instanceof Double)
				return NumericUtils.doubleToPrefixCoded((Double) n);
			else
				throw new IllegalArgumentException("Unhandled numeric type: " + n.getClass());
		} else {
			throw new IllegalArgumentException("Value is not a number: " + value);
		}
	}

}