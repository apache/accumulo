package org.apache.accumulo.core.data;

public enum PartialKey {
	ROW(1),
	ROW_COLFAM(2),
	ROW_COLFAM_COLQUAL(3),
	ROW_COLFAM_COLQUAL_COLVIS(4),
	ROW_COLFAM_COLQUAL_COLVIS_TIME(5),
	ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL(6) // everything with delete flag
	;
	
	int depth;
	private PartialKey(int depth) { this.depth = depth; }
	public static PartialKey getByDepth(int depth)
	{
		for (PartialKey d : PartialKey.values())
			if (depth == d.depth)
				return d;
		throw new IllegalArgumentException("Invalid legacy depth " + depth);
	}
	public int getDepth()
	{
		return depth;
	}
}
