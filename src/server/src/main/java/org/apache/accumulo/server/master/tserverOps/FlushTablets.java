package org.apache.accumulo.server.master.tserverOps;

import java.util.Collection;

import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.EventCoordinator.Listener;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.master.tableOps.MasterRepo;
import org.apache.log4j.Logger;


public class FlushTablets extends MasterRepo {

    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(FlushTablets.class);
    String logger;

    public FlushTablets(String logger) {
        this.logger = logger;
    }

    @Override
	public long isReady(long tid, Master environment) throws Exception {
		return 0;
	}
    
    @Override
    public Repo<Master> call(long tid, Master m) throws Exception {
    	//TODO move this code to isReady() and drop while loop?
        // Make sure nobody is still using logs hosted on that node
        Listener listener = m.getEventCoordinator().getListener();
        while (m.stillMaster()) {
            boolean flushed = false;
            ZooTabletStateStore zooTabletStateStore = null;
            try {
                zooTabletStateStore = new ZooTabletStateStore();
            } catch (DistributedStoreException e) {
                log.warn("Unable to open ZooTabletStateStore, will retry", e);
            }
            MetaDataStateStore theRest = new MetaDataStateStore(null);
            for (TabletStateStore store : new TabletStateStore[]{zooTabletStateStore, theRest} ) {
                if (store != null) {
                    for (TabletLocationState tabletState : store) {
                        for (Collection<String> logSet : tabletState.walogs) {
                            for (String logEntry : logSet) {
                                if (logger.equals(logEntry.split("/")[0])) {
                                    TServerConnection tserver = m.getConnection(tabletState.current);
                                    if (tserver != null) {
                                        log.info("Requesting " + tabletState.current + " flush tablet " + tabletState.extent + " because it has a log entry " + logEntry);
                                        tserver.flushTablet(m.getMasterLock(), tabletState.extent);
                                    }
                                    flushed = true;
                                }
                            }
                        }
                    }
                }
            }
            if (zooTabletStateStore != null && !flushed)
                break;
            listener.waitForEvents(1000);
        }
        return new StopLogger(logger);
    }

    @Override
    public void undo(long tid, Master m) throws Exception {
    }

}
