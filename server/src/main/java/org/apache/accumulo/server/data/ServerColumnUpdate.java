package org.apache.accumulo.server.data;

import org.apache.accumulo.core.data.ColumnUpdate;

public class ServerColumnUpdate extends ColumnUpdate {
  
  ServerMutation parent;

  public ServerColumnUpdate(ColumnUpdate update, ServerMutation serverMutation) {
    super(update.getColumnFamily(), update.getColumnQualifier(), update.getColumnVisibility(), update.hasTimestamp(), update.hasTimestamp() ? update.getTimestamp() : 0, update.isDeleted(), update.getValue());
    parent = serverMutation;
  }

  public long getTimestamp() {
    if (hasTimestamp())
      return super.getTimestamp();
    return parent.getSystemTimestamp();
  }

}
