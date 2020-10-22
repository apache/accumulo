package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * Tablet ASSIGNED_TO_DEAD_SERVER with a goal of not hosted.  The work is the same no matter the
 * Goal (UNASSIGNED, DELETED, or SUSPENDED)
 */
public class DeadTabletNotHostedGoal extends AbstractGoal {
  final Goal.State goal;

  public DeadTabletNotHostedGoal(Master m, TabletLocationState tls, Goal.State goal) {
    super(m, tls);
    this.goal = goal;
  }

  @Override
  public State get() {
    return goal;
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.ASSIGNED_TO_DEAD_SERVER;
  }

  @Override
  public void workTowardsGoal() {

  }
}
