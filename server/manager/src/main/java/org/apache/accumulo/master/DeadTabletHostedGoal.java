package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

public class DeadTabletHostedGoal extends AbstractHostedGoal{

  public DeadTabletHostedGoal(Master m, TabletLocationState tls) {
    super(m, tls);
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.ASSIGNED_TO_DEAD_SERVER;
  }

  @Override
  public void workTowardsGoal() {

  }
}
