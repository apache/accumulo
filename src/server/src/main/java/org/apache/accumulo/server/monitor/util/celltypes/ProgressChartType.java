package org.apache.accumulo.server.monitor.util.celltypes;

public class ProgressChartType extends NumberType<Double> {

	private double max;

	public ProgressChartType() {
		this(1.0);
	}

	public ProgressChartType(Double total) {
		max = total == null ? 1.0 : total;
	}

	@Override
	public String format(Object obj) {
		if (obj == null)
			return "-";
		Double num = (Double) obj;
		return getChart(num, max);
	}

	public static String getChart(double num, double total) {
		StringBuilder result = new StringBuilder();
		double percent = 0;
		if (total != 0)
			percent = (num / total) * 100;

		int width = 0;
		if (percent < 1)
			width = 0;
		else if (percent > 100)
			width = 100;
		else
			width = (int) percent;

		result.append("<div class='progress-chart'>");
		result.append("<div style='width: ").append(width).append("%;'></div>");
		result.append("</div>&nbsp;");
		result.append((percent < 1 && percent > 0) ? "&lt;1" : (int) percent).append("%");
		return result.toString();
	}
}
