package org.apache.accumulo.server.master.tableOps;

import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.state.tables.TableManager;
import org.apache.log4j.Logger;


public class ChangeTableState extends MasterRepo {

	private static final long serialVersionUID = 1L;
	private String tableId;
	private TableOperation top;

	public ChangeTableState(String tableId, TableOperation top){
		this.tableId = tableId;
		this.top = top;
		
		if(top != TableOperation.ONLINE && top != TableOperation.OFFLINE)
			throw new IllegalArgumentException(top.toString());
	}
	
	@Override
	public long isReady(long tid, Master environment) throws Exception {
		//reserve the table so that this op does not run concurrently with create, clone, or delete table
		return Utils.reserveTable(tableId, tid, true, true, top);
	}
	
	@Override
	public Repo<Master> call(long tid, Master env) throws Exception {
		
		TableState ts = TableState.ONLINE;
		if(top == TableOperation.OFFLINE)
			ts = TableState.OFFLINE;
		
		
		TableManager.getInstance().transitionTableState(tableId, ts);
        Utils.unreserveTable(tableId, tid, true); 
        Logger.getLogger(ChangeTableState.class).debug("Changed table state "+tableId+" "+ts);
        env.getEventCoordinator().event("Set table state of %s to %s", tableId, ts);
		return null;
	}

	@Override
	public void undo(long tid, Master env) throws Exception {
		Utils.unreserveTable(tableId, tid, true);
	}
}
