package org.apache.accumulo.server.monitor.util.celltypes;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateTimeType extends CellType<Long> {
	private SimpleDateFormat simple;
	private int dateFormat;
	private int timeFormat;

	public DateTimeType(int dateFormat, int timeFormat) {
		this.dateFormat = dateFormat;
		this.timeFormat = timeFormat;
		this.simple = null;
	}

	public DateTimeType(SimpleDateFormat fmt) {
		simple = fmt;
	}

	@Override
	public String format(Object obj) {
		if (obj == null)
			return "-";
		Long millis = (Long) obj;
		if (millis == 0)
			return "-";
		if (simple != null)
			return simple.format(new Date(millis)).replace(" ", "&nbsp;");
		return DateFormat.getDateTimeInstance(dateFormat, timeFormat, Locale.getDefault()).format(new Date(millis)).replace(" ", "&nbsp;");
	}

	@Override
	public int compare(Long o1, Long o2) {
		if (o1 == null && o2 == null)
			return 0;
		else if (o1 == null)
			return -1;
		else
			return o1.compareTo(o2);
	}

	@Override
	public String alignment() {
		return "right";
	}

}
