package org.apache.accumulo.server.master.state;

public enum MergeState {
    /**
     *  Not merging
     */
    NONE,
    /**
     * created, stored in zookeeper, other merges are prevented on the table
     */
    STARTED,
    /**
     * put all matching tablets online, split tablets if we are deleting
     */
    SPLITTING,
    /**
     * after the tablet server chops the file, it marks the metadata table with a chopped marker
     */
    WAITING_FOR_CHOPPED,
    /**
     * when the number of chopped tablets in the range matches the number of online tablets in the range, 
     * take the tablets offline
     */
    WAITING_FOR_OFFLINE,
    /**
     * when the number of chopped, offline tablets equals the number of merge tablets, begin the metadata updates
     */
    MERGING,
    /** 
     * merge is complete, the resulting tablet can be brought online,
     * remove the marker in zookeeper
     */
    COMPLETE;

}
