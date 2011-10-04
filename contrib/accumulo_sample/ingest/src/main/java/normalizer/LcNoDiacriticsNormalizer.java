package normalizer;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An {@link Normalizer} which performs the following steps:
 * <ol>
 *   <li>Unicode canonical decomposition ({@link Form#NFD})</li>
 *   <li>Removal of diacritical marks</li>
 *   <li>Unicode canonical composition ({@link Form#NFC})</li>
 *   <li>lower casing in the {@link Locale#ENGLISH English local}
 * </ol>
 */
public class LcNoDiacriticsNormalizer implements normalizer.Normalizer
{
	private static final Pattern diacriticals = Pattern.compile(
			"\\p{InCombiningDiacriticalMarks}");

	public String normalizeFieldValue(String fieldName, Object fieldValue) 
	{
		String decomposed = Normalizer.normalize(fieldValue.toString(), Form.NFD);
		String noDiacriticals = removeDiacriticalMarks(decomposed);
		String recomposed = Normalizer.normalize(noDiacriticals, Form.NFC);
		return recomposed.toLowerCase(Locale.ENGLISH);
	}

	private String removeDiacriticalMarks(String str) {
		Matcher matcher = diacriticals.matcher(str);
		return matcher.replaceAll("");
	}

}