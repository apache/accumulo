package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * Tablet UNASSIGNED with a goal of not hosted.  The work is the same no matter the
 * Goal (UNASSIGNED, DELETED, or SUSPENDED)
 */
public class UnassignedTabletNotHostedGoal extends AbstractGoal {
  final Goal.State goal;

  public UnassignedTabletNotHostedGoal(Master m, TabletLocationState tls, Goal.State goal) {
    super(m, tls);
    this.goal = goal;
  }

  @Override
  public Goal.State get() {
    return goal;
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.UNASSIGNED;
  }

  @Override
  public void workTowardsGoal() {
    cancelOfflineTableMigrations(tablet);
  }
}
