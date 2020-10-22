package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * A Tablet with a current state of SUSPENDED with a goal of hosted.
 */
public class SuspendedTabletHostedGoal extends AbstractHostedGoal {
  public SuspendedTabletHostedGoal(Master m, TabletLocationState tls) {
    super(m, tls);
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.SUSPENDED;
  }

  @Override
  public void workTowardsGoal() {

  }
}
