package org.apache.accumulo.server.master.state;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.accumulo.core.Constants;



/**
 * Interface for storing information about tablet assignments. There are three implementations:
 * 
 *  ZooTabletStateStore: information about the root tablet is stored in ZooKeeper
 *  MetaDataStateStore: information about the other tablets are stored in the metadata table
 *  
 */
public abstract class TabletStateStore implements Iterable<TabletLocationState> {
    
    /** 
     * Identifying name for this tablet state store.
     */
    abstract public String name();
    
    /**
     * Scan the information about the tablets covered by this store
     */
    abstract public Iterator<TabletLocationState> iterator();
    
    /**
     * Store the assigned locations in the data store.
     * @param extent The tablet definition
     * @param tabletSession the tablet server the tablet is to be assigned
     * @throws DistributedStoreException
     */
    abstract public void setFutureLocations(Collection<Assignment> assignments) throws DistributedStoreException;
    
    /**
     * Tablet servers will update the data store with the location when they bring the tablet online
     * @param extent the tablet definition
     * @param tabletSession the tablet server that opened the tablet 
     * @throws DistributedStoreException 
     */
    abstract public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException;
  

    /**
     * Mark the tablets as having no known or future location.
     * @param extent the tablets' current information
     * @throws DistributedStoreException
     */
    abstract public void unassign(Collection<TabletLocationState> tablets) throws DistributedStoreException;

    public static void unassign(TabletLocationState tls) throws DistributedStoreException {
        TabletStateStore store;
        if (tls.extent.equals(Constants.ROOT_TABLET_EXTENT)) {
            store = new ZooTabletStateStore();
        } else {
            store = new MetaDataStateStore(null);
        }
        store.unassign(Collections.singletonList(tls));
    }
    
    public static void setLocation(Assignment assignment) throws DistributedStoreException {
        TabletStateStore store;
        if (assignment.tablet.equals(Constants.ROOT_TABLET_EXTENT)) {
            store = new ZooTabletStateStore();
        } else {
            store = new MetaDataStateStore(null);
        }
        store.setLocations(Collections.singletonList(assignment));
    }
    
}
