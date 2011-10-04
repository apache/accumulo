package org.apache.accumulo.server.monitor.util.celltypes;

public class PercentageType extends CellType<Double>{

	@Override
	public int compare(Double o1, Double o2) {
		return o1.compareTo(o2);
	}

	@Override
	public String alignment() {
		return "right";
	}

	@Override
	public String format(Object obj) {
		if(obj == null)
			return "-";
		
		return String.format("%.0f%s", 100 * (Double)obj, "%");
		
	}

}
