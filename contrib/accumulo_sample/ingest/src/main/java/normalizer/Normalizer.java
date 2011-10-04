package normalizer;


public interface Normalizer {

	/**
	 * Creates normalized content for ingest based upon implemented logic.
	 * @param field The field being normalized
	 * @param value The value to normalize
	 * @return a normalized value
	 */
	public String normalizeFieldValue(String field, Object value);

}
