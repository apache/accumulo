package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * A Tablet with a current state of UNASSIGNED with a goal of hosted.
 */
public class UnassignedTabletHostedGoal extends AbstractHostedGoal {
  public UnassignedTabletHostedGoal(Master m, TabletLocationState tls) {
    super(m, tls);
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.UNASSIGNED;
  }

  @Override
  public void workTowardsGoal() {

  }
}
