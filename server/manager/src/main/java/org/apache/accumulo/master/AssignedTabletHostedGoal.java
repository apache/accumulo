package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * A Tablet with a current state of ASSIGNED with a goal of hosted.
 */
public class AssignedTabletHostedGoal extends AbstractHostedGoal {
  public AssignedTabletHostedGoal(Master m, TabletLocationState tls) {
    super(m, tls);
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.ASSIGNED;
  }

  @Override
  public void workTowardsGoal() {

  }
}
