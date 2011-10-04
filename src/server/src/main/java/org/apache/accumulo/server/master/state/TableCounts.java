/**
 * 
 */
package org.apache.accumulo.server.master.state;


public class TableCounts {
    int counts[] = new int[TabletState.values().length];

    public int unassigned() {
        return counts[TabletState.UNASSIGNED.ordinal()];
    }

    public int assigned() {
        return counts[TabletState.ASSIGNED.ordinal()];
    }

    public int assignedToDeadServers() {
        return counts[TabletState.ASSIGNED_TO_DEAD_SERVER.ordinal()];
    }

    public int hosted() {
        return counts[TabletState.HOSTED.ordinal()];
    }
}