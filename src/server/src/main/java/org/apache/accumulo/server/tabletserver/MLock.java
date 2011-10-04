package org.apache.accumulo.server.tabletserver;

public class MLock {
    public static native int lockMemoryPages();
}
