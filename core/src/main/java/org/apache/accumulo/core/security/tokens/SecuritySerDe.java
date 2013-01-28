package org.apache.accumulo.core.security.tokens;

import org.apache.accumulo.core.client.AccumuloSecurityException;

public interface SecuritySerDe<T extends SecurityToken> {
	public byte[] serialize(T token) throws AccumuloSecurityException;
	public T deserialize(byte[] serializedToken) throws AccumuloSecurityException;
}
