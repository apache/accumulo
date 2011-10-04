package org.apache.accumulo.server.master.state;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.HdfsZooInstance;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;

public class SetGoalState {

    /**
     * Utility program that will change the goal state for the master from the command line.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || MasterGoalState.valueOf(args[0]) == null) {
            System.err.println("Usage: accumulo " + SetGoalState.class.getName() + " [NORMAL|SAFE_MODE|CLEAN_STOP]");
            System.exit(-1);
        }
        ZooUtil.putPersistentData(ZooUtil.getRoot(HdfsZooInstance.getInstance()) + Constants.ZMASTER_GOAL_STATE, args[0].getBytes(), NodeExistsPolicy.OVERWRITE);
    }

}
