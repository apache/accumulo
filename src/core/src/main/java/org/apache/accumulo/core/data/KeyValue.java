package org.apache.accumulo.core.data;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.accumulo.core.util.ByteBufferUtil.toBytes;

public class KeyValue implements Map.Entry<Key, Value> {
    
    public Key key;
    public byte[] value;

    public KeyValue(
            Key key,
            byte[] value)
          {
            this.key = key;
            this.value = value;
          }

    public KeyValue(
            Key key,
            ByteBuffer value)
          {
            this.key = key;
            this.value = toBytes(value);
          }

    @Override
    public Key getKey() {
        return key;
    }

    @Override
    public Value getValue() {
        return new Value(value);
    }

    @Override
    public Value setValue(Value value) {
        throw new UnsupportedOperationException();
    }

    public String toString(){
    	return key+" "+new String(value);
    }
    
}

