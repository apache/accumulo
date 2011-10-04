package org.apache.accumulo.server.tabletserver;

public class HoldTimeoutException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public HoldTimeoutException(String why) { super(why); }
}
