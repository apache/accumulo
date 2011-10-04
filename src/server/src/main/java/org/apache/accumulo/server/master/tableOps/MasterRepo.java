package org.apache.accumulo.server.master.tableOps;

import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.log4j.Logger;


public abstract class MasterRepo implements Repo<Master> {

    private static final long serialVersionUID = 1L;
    protected static final Logger log = Logger.getLogger(MasterRepo.class);

    @Override
    public long isReady(long tid, Master environment) throws Exception {
        return 0;
    }

    @Override
    public void undo(long tid, Master environment) throws Exception {
    }

    @Override
    public String getDescription() {
        return this.getClass().getSimpleName();
    }
    
    @Override
    public String getReturn() {
        return null;
    }

    @Override
    abstract public Repo<Master> call(long tid, Master environment) throws Exception;

}
