package org.apache.accumulo.core.util;

public class Duration {

	public static String format(long time) {
		return format(time, "&nbsp;");
	}
	
	public static String format(long time, String space) {
		return format(time, space, "&emdash;");
	}

	public static String format(long time, String space, String zero) {
		long ms, sec, min, hr, day, yr;
		ms = sec = min = hr = day = yr = -1;
		if (time == 0)
			return zero;
		ms = time % 1000;
		time /= 1000;
		if (time == 0)
			return String.format("%dms", ms);
		sec = time % 60;
		time /= 60;
		if (time == 0)
			return String.format("%ds"+space+"%dms", sec, ms);
		min = time % 60;
		time /= 60;
		if (time == 0)
			return String.format("%dm"+space+"%ds", min, sec);
		hr = time % 24;
		time /= 24;
		if (time == 0)
			return String.format("%dh"+space+"%dm", hr, min);
		day = time % 365;
		time /= 365;
		if (time == 0)
			return String.format("%dd"+space+"%dh", day, hr);
		yr = time;
		return String.format("%dy"+space+"%dd", yr, day);

	}

}
