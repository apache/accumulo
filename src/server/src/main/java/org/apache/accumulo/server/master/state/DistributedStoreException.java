package org.apache.accumulo.server.master.state;

public class DistributedStoreException extends Exception {
    
    private static final long serialVersionUID = 1L;

    public DistributedStoreException(String why) {
        super(why);
    }

    public DistributedStoreException(Exception cause) {
        super(cause);
    }
    
    public DistributedStoreException(String why, Exception cause) {
        super(why, cause);
    }
}

