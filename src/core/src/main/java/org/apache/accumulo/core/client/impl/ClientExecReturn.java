package org.apache.accumulo.core.client.impl;

public interface ClientExecReturn<T, C> {
    T execute(C client) throws Exception;
}
