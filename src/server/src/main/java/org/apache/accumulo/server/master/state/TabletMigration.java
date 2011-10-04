/**
 * 
 */
package org.apache.accumulo.server.master.state;

import org.apache.accumulo.core.data.KeyExtent;

public class TabletMigration {
    public KeyExtent tablet;
    public TServerInstance oldServer;
    public TServerInstance newServer;
    
    public TabletMigration(KeyExtent extent, TServerInstance before, TServerInstance after) {
        this.tablet = extent;
        this.oldServer = before;
        this.newServer = after;
    }
    
    public String toString() {
        return tablet + ": " + oldServer + " -> " + newServer;
    }
}