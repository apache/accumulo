package org.apache.accumulo.core.client.impl;

import org.apache.accumulo.core.util.ArgumentChecker;

class ThriftTransportKey
{
    private final String location;
    private final int port;
    private final long timeout;

    private int hash = -1;

    ThriftTransportKey(String location, int port, long timeout)
    {
        ArgumentChecker.notNull(location);
        this.location = location;
        this.port = port;
        this.timeout = timeout;
    }

    String getLocation()
    {
        return location;
    }

    int getPort()
    {
        return port;
    }

    long getTimeout()
    {
        return timeout;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ThriftTransportKey))
            return false;
        ThriftTransportKey ttk = (ThriftTransportKey) o;
        return location.equals(ttk.location) && port == ttk.port && timeout == ttk.timeout;
    }

    @Override
    public int hashCode()
    {
        if (hash == -1)
            hash = (location + Integer.toString(port) + Long.toString(timeout)).hashCode();
        return hash;
    }

    @Override
    public String toString()
    {
        return location + ":" + Integer.toString(port) + " (" + Long.toString(timeout) + ")";
    }
}
