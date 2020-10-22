package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * Tablet HOSTED with a goal of not hosted.  The work is the same no matter the
 * Goal (UNASSIGNED, DELETED, or SUSPENDED)
 */
public class HostedTabletNotHostedGoal extends AbstractGoal {
  final Goal.State goal;

  public HostedTabletNotHostedGoal(Master m, TabletLocationState tls, Goal.State goal) {
    super(m, tls);
    this.goal = goal;
  }

  @Override
  public Goal.State get() {
    return goal;
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.HOSTED;
  }

  @Override
  public void workTowardsGoal() {

  }
}
