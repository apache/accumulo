package org.apache.accumulo.core.client.impl;


public interface ClientExec<T> {
    void execute(T iface) throws Exception;
}
