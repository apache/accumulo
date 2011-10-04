package org.apache.accumulo.server.util;

import java.net.InetSocketAddress;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.conf.ServerConfiguration;


public class AddressUtil {
    static public InetSocketAddress parseAddress(String address, Property portDefaultProperty) {
        final int dfaultPort = ServerConfiguration.getSystemConfiguration().getPort(Property.TSERV_CLIENTPORT);
        return org.apache.accumulo.core.util.AddressUtil.parseAddress(address, dfaultPort);
    }

}
