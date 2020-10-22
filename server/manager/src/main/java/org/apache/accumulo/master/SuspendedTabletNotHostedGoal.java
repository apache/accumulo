package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * Tablet SUSPENDED with a goal of not hosted.  The work is the same no matter the
 * Goal (UNASSIGNED, DELETED, or SUSPENDED)
 */
public class SuspendedTabletNotHostedGoal extends AbstractGoal {
  final Goal.State goal;

  public SuspendedTabletNotHostedGoal(Master m, TabletLocationState tls, Goal.State goal) {
    super(m, tls);
    this.goal = goal;
  }

  @Override
  public Goal.State get() {
    return goal;
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.SUSPENDED;
  }

  @Override
  public void workTowardsGoal() {
    // Request a move to UNASSIGNED, so as to allow balancing to continue.
    //suspendedToGoneServers.add(tls);
    cancelOfflineTableMigrations(tablet);
  }
}
