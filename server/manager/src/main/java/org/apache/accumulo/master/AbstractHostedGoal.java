package org.apache.accumulo.master;

import org.apache.accumulo.server.master.state.TabletLocationState;

/**
 * A Goal of being hosted. Concrete types are defined based on current tablet state.
 */
public abstract class AbstractHostedGoal extends AbstractGoal {
  final Goal.State goal;

  public AbstractHostedGoal(Master m, TabletLocationState tls) {
    super(m, tls);
    this.goal = State.HOSTED;
  }

  @Override
  public State get() {
    return goal;
  }

  @Override
  public String toString() {
    return goal.toString();
  }
}
