package org.apache.accumulo.master;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletState;

public abstract class AbstractGoal implements Goal {
  Master master;
  KeyExtent tablet;
  TServerInstance location;

  public AbstractGoal(Master m, TabletLocationState tls) {
    this.master = m;
    this.tablet = tls.extent;
    this.location = tls.getServer();
  }

  @Override
  public abstract State get();

  @Override
  public abstract TabletState getCurrentState();

  @Override
  public KeyExtent getTablet() {
    return tablet;
  }

  @Override
  public abstract void workTowardsGoal();

  void cancelOfflineTableMigrations(KeyExtent tablet) {
    TServerInstance dest = master.migrations.get(tablet);
    TableState tableState = master.getTableManager().getTableState(tablet.tableId());
    if (dest != null && tableState == TableState.OFFLINE) {
      master.migrations.remove(tablet);
    }
  }
}
