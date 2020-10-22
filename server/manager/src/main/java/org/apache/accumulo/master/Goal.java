package org.apache.accumulo.master;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.master.state.TabletState;

public interface Goal {
  /**
   * Pulled from the old TabletGoalState
   */
  enum State {
    HOSTED,
    UNASSIGNED, // aka NotHosted
    DELETED, // aka NotHosted
    SUSPENDED // aka NotHosted
  }

  /**
   * Return this goal state.
   */
  State get();

  /**
   * Return the current state of the Tablet.
   */
  TabletState getCurrentState();

  /**
   * Return the tablet associated with this Goal.
   */
  KeyExtent getTablet();

  /**
   * Take the actions need to migrate the Tablet from current state to the Goal.
   */
  void workTowardsGoal(/* Pass object containing tablet lists and counts */);
}
