package org.apache.accumulo.server.master.state.tables;

import java.util.Map;

import org.apache.accumulo.core.master.state.tables.TableState;



public interface TableObserver
{
    void initialize(Map<String, TableState> tableIdToStateMap);
    void stateChanged(String tableId, TableState tState);
    void sessionExpired();
}
