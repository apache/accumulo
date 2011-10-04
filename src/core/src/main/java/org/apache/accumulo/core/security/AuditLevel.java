package org.apache.accumulo.core.security;

import org.apache.log4j.Level;

public class AuditLevel extends Level {

    private static final long serialVersionUID = 1L;
    public final static Level AUDIT = new AuditLevel();
    
    protected AuditLevel() {
        super(Level.INFO_INT + 100, "AUDIT", Level.INFO_INT + 100);
    }
    
    static public Level toLevel(int val) {
        if (val == Level.INFO_INT + 100) return Level.INFO;
        return Level.toLevel(val);
    }
}