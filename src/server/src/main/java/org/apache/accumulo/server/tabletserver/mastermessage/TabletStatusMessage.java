package org.apache.accumulo.server.tabletserver.mastermessage;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.MasterClientService.Iface;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.thrift.TException;


public class TabletStatusMessage implements MasterMessage {
    
    private KeyExtent extent;
    private TabletLoadState status;

    public TabletStatusMessage(TabletLoadState status, KeyExtent extent)
    {
        this.extent = extent;
        this.status = status;
    }

    public void send(AuthInfo auth, String serverName, Iface client) throws TException, ThriftSecurityException {
        client.reportTabletStatus(null, auth, serverName, status, extent.toThrift());
    }
}
