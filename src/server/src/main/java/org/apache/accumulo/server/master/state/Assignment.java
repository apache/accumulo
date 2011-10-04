package org.apache.accumulo.server.master.state;

import org.apache.accumulo.core.data.KeyExtent;

public class Assignment {
    public KeyExtent tablet;
    public TServerInstance server;
    
    public Assignment(KeyExtent tablet, TServerInstance server) {
        this.tablet = tablet;
        this.server = server;
    }
}
