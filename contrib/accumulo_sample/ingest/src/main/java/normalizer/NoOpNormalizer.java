package normalizer;

public class NoOpNormalizer implements Normalizer
{
	public String normalizeFieldValue(String field, Object value) {
		return value.toString();
	}
}
