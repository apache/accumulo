package org.apache.accumulo.server.monitor.util.celltypes;

import org.apache.accumulo.core.util.Duration;

public class DurationType extends NumberType<Long> {
	private Long errMin;
	private Long errMax;

	public DurationType() {
		this(null, null);
	}

	public DurationType(Long errMin, Long errMax) {
		this.errMin = errMin;
		this.errMax = errMax;
	}

	@Override
	public String format(Object obj) {
		if (obj == null)
			return "-";
		Long millis = (Long) obj;
		if (errMin != null && errMax != null)
			return seconds(millis, errMin, errMax);
		return Duration.format(millis);
	}

	private static String seconds(long secs, long errMin, long errMax) {
		String numbers = Duration.format(secs);
		if (secs < errMin || secs > errMax)
			return "<span class='error'>" + numbers + "</span>";
		return numbers;
	}

}
