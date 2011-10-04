package org.apache.accumulo.server.master.state;

import java.util.Collection;
import java.util.Set;

public interface CurrentState {
    
    Set<String> onlineTables();
    
    Set<TServerInstance> onlineTabletServers();
    
    Collection<MergeInfo> merges();

}
