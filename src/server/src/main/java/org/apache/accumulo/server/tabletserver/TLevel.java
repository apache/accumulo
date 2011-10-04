package org.apache.accumulo.server.tabletserver;

import org.apache.log4j.Level;

public class TLevel extends Level {

	private static final long serialVersionUID = 1L;
	public final static Level TABLET_HIST = new TLevel();
	
	protected TLevel() {
		super(Level.DEBUG_INT + 100, "TABLET_HIST", Level.DEBUG_INT + 100);
	}
	
	static public Level toLevel(int val) {
	    if (val == Level.DEBUG_INT + 100) return Level.DEBUG;
	    return Level.toLevel(val);
	}

}
