package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

/**
 * A Tablet with a current state of hosted with a goal of hosted.
 */
public class HostedTabletHostedGoal extends AbstractHostedGoal {

  public HostedTabletHostedGoal(Master m, TabletLocationState tls) {
    super(m, tls);
  }

  @Override
  public void workTowardsGoal() {
    if (location.equals(master.migrations.get(tablet)))
      master.migrations.remove(tablet);
  }

  @Override
  public TabletState getCurrentState() {
    return TabletState.HOSTED;
  }
}
