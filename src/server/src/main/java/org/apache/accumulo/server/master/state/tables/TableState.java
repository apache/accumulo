package org.apache.accumulo.server.master.state.tables;


public enum TableState
{
    // NEW while making directories and tablets;
    // NEW transitions to LOADING
    NEW,

    // LOADING if tablets need assigned, and during assignment;
    // LOADING transitions to DISABLING, UNLOADING, DELETING, ONLINE
    LOADING,
    // ONLINE when all tablets are assigned (or suspected to be);
    // ONLINE transitions to LOADING, DISABLING, UNLOADING, DELETING
    ONLINE,

    // DISABLING when unloading tablets for DISABLED;
    // DISABLING transitions to DISABLED
    DISABLING,
    // DISABLED is like OFFLINE but won't come up on startup;
    // DISABLED transitions to LOADING, DELETING
    DISABLED,

    // UNLOADING when unloading tablets for OFFLINE;
    // UNLOADING transitions to OFFLINE
    UNLOADING,
    // OFFLINE is offline but will come up on startup (unless in safemode);
    // OFFLINE transitions to DISABLED, DELETING, LOADING (only in safemode)
    OFFLINE,

    // DELETING when unloading tablets, and cleaning up filesystem and metadata;
    // DELETING transitions to nothingness
    DELETING,

    // UNKNOWN is NOT a valid state; it is reserved for unrecognized serialized
    // representations of table state
    UNKNOWN;
}
