package org.apache.accumulo.server.data;

import org.apache.accumulo.core.data.ColumnUpdate;

public class ServerColumnUpdate extends ColumnUpdate {
  
  ServerMutation parent;

  public ServerColumnUpdate(byte[] cf, byte[] cq, byte[] cv, boolean hasts, long ts, boolean deleted, byte[] val, ServerMutation serverMutation) {
    super(cf, cq, cv, hasts, ts, deleted, val);
    parent = serverMutation;
  }

  public long getTimestamp() {
    if (hasTimestamp())
      return super.getTimestamp();
    return parent.getSystemTimestamp();
  }

}
