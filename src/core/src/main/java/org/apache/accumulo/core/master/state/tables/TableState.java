package org.apache.accumulo.core.master.state.tables;


public enum TableState
{
    // NEW while making directories and tablets;
    NEW,

    // ONLINE tablets will be assigned
    ONLINE,

    // OFFLINE tablets will be taken offline
    OFFLINE,

    // DELETING waiting for tablets to go offline and table will be removed
    DELETING,

    // UNKNOWN is NOT a valid state; it is reserved for unrecognized serialized
    // representations of table state
    UNKNOWN;
}
