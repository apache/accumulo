/**
 * 
 */
package org.apache.accumulo.server.master.state;

public enum TabletState {
    UNASSIGNED, ASSIGNED, HOSTED, ASSIGNED_TO_DEAD_SERVER
}